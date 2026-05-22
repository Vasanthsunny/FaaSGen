from fastapi import FastAPI, Request
import redis
import time
import psutil
import json
import cv2
import numpy as np
import requests
import uvicorn
peak_memory = 0
app = FastAPI()

r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=False
)

F3_URL = "http://img-f3-edge-detect.default.svc.cluster.local"

def record(job_id, name, arrival, start, end, cpu_total,peak_memory):


    metrics = {
        "job_id": job_id,
        "function": "img-f2-resize-blur",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end-start)*1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory/(1024*1024)
    }

    r.set(f"job:{job_id}:{name}", json.dumps(metrics))


@app.post("/")
async def resize_blur(request: Request):
    global peak_memory
    peak_memory = 0  # reset peak memory for this request
    arrival = time.time()
    data = await request.json()
    job_id = data["job_id"]

    raw = r.get(f"job:{job_id}:raw")

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()
    peak_memory = process.memory_info().rss
    np_arr = np.frombuffer(raw, np.uint8)
    img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

    img = cv2.resize(img, (1024,1024))
    peak_memory = max(peak_memory, process.memory_info().rss)
    img = cv2.GaussianBlur(img, (7,7), 0)
    peak_memory = max(peak_memory, process.memory_info().rss)
    _, buffer = cv2.imencode(".png", img)
    r.set(f"job:{job_id}:blur", buffer.tobytes())

    end = time.time()
    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user)
        + (cpu_end.system - cpu_start.system)
    )

    record(job_id, "img-f2-resize-blur", arrival, start, end, cpu_total, peak_memory)

    requests.post(F3_URL, json={"job_id": job_id})

    return {"status":"processed"}

