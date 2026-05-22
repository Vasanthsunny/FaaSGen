from fastapi import FastAPI, Request
import uvicorn
import redis
import json
import time
import psutil
import uuid
import base64
import requests
peak_memory = 0
app = FastAPI()

r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=True
)

F2_URL = "http://log-f2-shard.default.svc.cluster.local"

def record_metrics(job_id, arrival, start, end, cpu_total, peak_memory):
    r.set(
        f"job:{job_id}:log-f1-batch",
        json.dumps({
            "job_id": job_id,
            "function": "log-f1-batch",
            "arrival_time": arrival,
            "start_time": start,
            "end_time": end,
            "processing_time_ms": (end-start)*1000,
            "cpu_time_sec": cpu_total,
            "memory_mb": peak_memory / (1024 * 1024)
        })
    )

@app.post("/")
async def batch(request: Request):
    global peak_memory
    peak_memory = 0
    arrival = time.time()
    data = await request.json()
    log_b64 = data["log"]

    job_id = str(uuid.uuid4())

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()
    peak_memory = process.memory_info().rss
    raw_logs = base64.b64decode(log_b64).decode()
    peak_memory = max(peak_memory, process.memory_info().rss)
    lines = raw_logs.splitlines()

    r.set(f"job:{job_id}:raw_logs", json.dumps(lines))
    
    end = time.time()
    cpu_end = process.cpu_times()

    cpu_total = (cpu_end.user-cpu_start.user)+(cpu_end.system-cpu_start.system)

    record_metrics(job_id, arrival, start, end, cpu_total, peak_memory)

    
    requests.post(F2_URL, json={"job_id": job_id})

    return {"job_id": job_id, "status": "batched"}


