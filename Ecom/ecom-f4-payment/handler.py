from fastapi import FastAPI, Request
import redis
import time
import psutil
import json
import requests
import random
import uvicorn

app = FastAPI()

r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=True
)

F5_URL = "http://ecom-f5-combine.default.svc.cluster.local"

def record_metrics(job_id, arrival, start, end, cpu_total):
    process = psutil.Process()
    mem = process.memory_info().rss / (1024 * 1024)

    metrics = {
        "job_id": job_id,
        "function": "ecom-f4-payment",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end-start)*1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": mem
    }

    r.set(f"job:{job_id}:ecom-f4-payment", json.dumps(metrics))


@app.post("/")
async def payment(request: Request):

    arrival = time.time()
    data = await request.json()
    job_id = data["job_id"]

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()

    # Simulate payment processing latency
    time.sleep(1.2)

    payment_success = random.choice([True, True, True, False])

    r.set(f"job:{job_id}:payment",
          json.dumps({"success": payment_success}))

    end = time.time()
    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user)
        + (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, arrival, start, end, cpu_total)

    # Trigger combine
    try:
        requests.post(F5_URL, json={"job_id": job_id})
    except:
        pass

    return {"job_id": job_id, "payment_processed": payment_success}

