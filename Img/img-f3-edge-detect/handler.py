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

F4_URL = "http://img-f4-aggregate-output.default.svc.cluster.local"

def record(job_id, name, arrival, start, end, cpu_total,peak_memory):


    metrics = {
        "job_id": job_id,
        "function": "img-f3-edge-detect",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end-start)*1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory/(1024*1024)
    }

    r.set(f"job:{job_id}:{name}", json.dumps(metrics))


@app.post("/")
async def edge_detect(request: Request):
    global peak_memory
    peak_memory = 0  # reset peak memory for this request
    arrival = time.time()
    data = await request.json()
    job_id = data["job_id"]

    raw = r.get(f"job:{job_id}:blur")

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()
    peak_memory = process.memory_info().rss
    np_arr = np.frombuffer(raw, np.uint8)
    img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
    peak_memory = max(peak_memory, process.memory_info().rss)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    sobel = cv2.Sobel(gray, cv2.CV_64F, 1, 1, ksize=5)
    peak_memory = max(peak_memory, process.memory_info().rss)
    edges = cv2.Canny(gray, 100, 200)

    combined = cv2.addWeighted(gray, 0.5, edges, 0.5, 0)
    peak_memory = max(peak_memory, process.memory_info().rss)
    _, buffer = cv2.imencode(".png", combined)
    r.set(f"job:{job_id}:edges", buffer.tobytes())

    end = time.time()
    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user)
        + (cpu_end.system - cpu_start.system)
    )

    record(job_id, "img-f3-edge-detect", arrival, start, end, cpu_total,peak_memory)

    requests.post(F4_URL, json={"job_id": job_id})

    return {"status":"edges_done"}

