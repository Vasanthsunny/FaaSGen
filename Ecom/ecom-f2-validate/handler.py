from fastapi import FastAPI, Request
import redis
import time
import psutil
import json
import requests
import uvicorn

app = FastAPI()

r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=True
)

F3_URL = "http://ecom-f3-inventory.default.svc.cluster.local"
F4_URL = "http://ecom-f4-payment.default.svc.cluster.local"


def record_metrics(job_id, arrival, start, end, cpu_total):
    process = psutil.Process()
    mem = process.memory_info().rss / (1024 * 1024)

    metrics = {
        "job_id": job_id,
        "function": "ecom-f2-validate",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": mem
    }

    r.set(f"job:{job_id}:ecom-f2-validate", json.dumps(metrics))


@app.post("/")
async def validate(request: Request):

    arrival = time.time()
    data = await request.json()
    job_id = data["job_id"]

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()

    order_raw = r.get(f"job:{job_id}:order")
    if not order_raw:
        return {"status": "order_not_found"}

    order = json.loads(order_raw)

    valid = (
        "items" in order and
        "total_amount" in order and
        isinstance(order["items"], list)
    )

    r.set(f"job:{job_id}:validated", json.dumps({"valid": valid}))

    end = time.time()
    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user)
        + (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, arrival, start, end, cpu_total)

    if valid:
        requests.post(F3_URL, json={"job_id": job_id})
        requests.post(F4_URL, json={"job_id": job_id})

    return {"job_id": job_id, "status": "validated"}

