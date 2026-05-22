from fastapi import FastAPI, Request
import redis
import uuid
import time
import psutil
import json
import requests
import uvicorn

app = FastAPI()

# Redis
r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=True
)

F2_URL = "http://ecom-f2-validate.default.svc.cluster.local"


def record_metrics(job_id, arrival, start, end, cpu_total):
    process = psutil.Process()
    mem = process.memory_info().rss / (1024 * 1024)

    metrics = {
        "job_id": job_id,
        "function": "ecom-f1-receive",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": mem
    }

    r.set(f"job:{job_id}:ecom-f1-receive", json.dumps(metrics))


@app.post("/")
async def receive(request: Request):

    arrival = time.time()
    job_id = str(uuid.uuid4())

    order = await request.json()

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()

    r.set(f"job:{job_id}:order", json.dumps(order))

    end = time.time()
    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user)
        + (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, arrival, start, end, cpu_total)

    try:
        requests.post(F2_URL, json={"job_id": job_id})
    except:
        pass

    return {"job_id": job_id, "status": "received"}

