from fastapi import FastAPI, Request
import redis
import time
import psutil
import json
import uuid
import gzip
import requests
import os
import uvicorn

peak_memory = 0
app = FastAPI()

r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=False
)

F2_URL = "http://ml-f2-split.default.svc.cluster.local"


def record_metrics(job_id, arrival, start, end, cpu_total,peak_memory):


    metrics = {
        "job_id": job_id,
        "function": "ml-f1-init",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end-start)*1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(
        f"job:{job_id}:ml-f1-init",
        json.dumps(metrics)
    )


@app.post("/")
async def upload_dataset(request: Request):

    global peak_memory
    peak_memory = 0
    arrival = time.time()
    job_id = str(uuid.uuid4())

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()
    peak_memory = process.memory_info().rss
    dataset_bytes = await request.body()

    # compress before storing
    
    r.set(f"job:{job_id}:dataset",dataset_bytes)
    peak_memory = max(peak_memory, process.memory_info().rss)
    end = time.time()
    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user)
        + (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, arrival, start, end, cpu_total, peak_memory)

    try:
        requests.post(F2_URL, json={"job_id": job_id})
    except:
        pass

    return {
        "job_id": job_id,
        "dataset_size_MB": round(len(dataset_bytes)/1024/1024,2),
        "status": "dataset_received"
    }
