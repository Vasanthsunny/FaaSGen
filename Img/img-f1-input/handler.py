from fastapi import FastAPI, Request
import redis
import time
import psutil
import json
import base64
import uuid
import requests
import uvicorn
peak_memory = 0
app = FastAPI()

r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=False
)

F2_URL = "http://img-f2-resize-blur.default.svc.cluster.local"

def record(job_id, name, arrival, start, end, cpu_total,peak_memory):

    metrics = {
        "job_id": job_id,
        "function": "img-f1-input",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end-start)*1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory/(1024*1024)
    }

    r.set(f"job:{job_id}:{name}", json.dumps(metrics))


@app.post("/")
async def input_stage(request: Request):
    global peak_memory
    peak_memory = 0  # reset peak memory for this request
    arrival = time.time()
    job_id = str(uuid.uuid4())
    data = await request.json()

    image_b64 = data["image"]
    image_bytes = base64.b64decode(image_b64)

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()
    peak_memory = process.memory_info().rss
    r.set(f"job:{job_id}:raw", image_bytes)
    peak_memory = max(peak_memory, process.memory_info().rss)
    end = time.time()
    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user)
        + (cpu_end.system - cpu_start.system)
    )

    record(job_id, "img-f1-input", arrival, start, end, cpu_total, peak_memory)

    requests.post(F2_URL, json={"job_id": job_id})

    return {"job_id": job_id, "status": "stored"}
    
