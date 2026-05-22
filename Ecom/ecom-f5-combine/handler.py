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

F6_URL = "http://ecom-f6-notify.default.svc.cluster.local"

def record_metrics(job_id, arrival, start, end, cpu_total):
    process = psutil.Process()
    mem = process.memory_info().rss / (1024 * 1024)

    metrics = {
        "job_id": job_id,
        "function": "ecom-f5-combine",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end-start)*1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": mem
    }

    r.set(f"job:{job_id}:ecom-f5-combine", json.dumps(metrics))


@app.post("/")
async def combine(request: Request):

    arrival = time.time()
    data = await request.json()
    job_id = data["job_id"]

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()

    inventory_raw = r.get(f"job:{job_id}:inventory")
    payment_raw = r.get(f"job:{job_id}:payment")

    # If one branch not ready yet → exit
    if not inventory_raw or not payment_raw:
        return {"job_id": job_id, "status": "waiting"}

    inventory = json.loads(inventory_raw)
    payment = json.loads(payment_raw)

    approved = inventory["available"] and payment["success"]

    r.set(f"job:{job_id}:approved", json.dumps({"approved": approved}))

    end = time.time()
    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user)
        + (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, arrival, start, end, cpu_total)

    # Trigger final notify
    try:
        requests.post(F6_URL, json={"job_id": job_id})
    except:
        pass

    return {"job_id": job_id, "approved": approved}



