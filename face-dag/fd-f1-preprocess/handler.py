from fastapi import FastAPI, Request
import numpy as np
import cv2
import base64
import io
import time
import uuid
import psutil
import redis
import json
import requests
import uvicorn
peak_memory = 0 
app = FastAPI()

# Redis connection
r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=True
)

F2_URL = "http://fd-f2-backbone.default.svc.cluster.local"


def record_metrics(job_id, function_name, arrival, start, end, cpu_total, peak_memory):

    metrics = {
        "job_id": job_id,
        "function": function_name,
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(f"job:{job_id}:{function_name}", json.dumps(metrics))


@app.post("/")
async def preprocess(request: Request):
    global peak_memory
    peak_memory = 0  # reset peak memory for this request
    arrival_time = time.time()
    job_id = str(uuid.uuid4())

    data = await request.json()
    image_b64 = data["image"]

    process = psutil.Process()

    # ---- CPU START ----
    cpu_start = process.cpu_times()
    start_time = time.time()
    peak_memory = process.memory_info().rss
    # ---- Decode image ----
    image_bytes = base64.b64decode(image_b64)
    np_arr = np.frombuffer(image_bytes, np.uint8)
    peak_memory = max(peak_memory, process.memory_info().rss)
    img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

    img = cv2.resize(img, (640, 640))
    peak_memory = max(peak_memory, process.memory_info().rss)
    img = img.transpose(2, 0, 1)  # CHW

    buf = io.BytesIO()
    np.save(buf, img)
    encoded = base64.b64encode(buf.getvalue()).decode()
    peak_memory = max(peak_memory, process.memory_info().rss)
    # ---- CPU END ----
    end_time = time.time()
    cpu_end = process.cpu_times()

    cpu_user = cpu_end.user - cpu_start.user
    cpu_system = cpu_end.system - cpu_start.system
    cpu_total = cpu_user + cpu_system

    peak_memory = max(peak_memory, process.memory_info().rss)

    # ---- Store metrics ----
    record_metrics(job_id,"fd-f1-preprocess",arrival_time,start_time,end_time,cpu_total,peak_memory)

    # ---- Async call to F2 ----
    payload = {"job_id": job_id,"tensor": encoded}

    try:
        requests.post(F2_URL, json=payload)
    except:
        pass

    return {
        "job_id": job_id,
        "status": "submitted"
    }


