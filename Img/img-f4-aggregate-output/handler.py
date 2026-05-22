from fastapi import FastAPI, Request
import redis
import time
import psutil
import json
import cv2
import numpy as np
import base64
import uvicorn
peak_memory = 0
app = FastAPI()

r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=False
)

def record_metrics(job_id, arrival, start, end, cpu_total, mem,peak_memory):
    metrics = {
        "job_id": job_id,
        "function": "img-f4-aggregate-output",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory/(1024*1024)
    }
    r.set(f"job:{job_id}:img-f4-aggregate-output", json.dumps(metrics))


@app.post("/")
async def aggregate_output(request: Request):
    global peak_memory
    peak_memory = 0  # reset peak memory for this request
    arrival_time = time.time()
    data = await request.json()
    job_id = data["job_id"]

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start_time = time.time()
    peak_memory = process.memory_info().rss
    # ---- Load edge image ----
    raw = r.get(f"job:{job_id}:edges")

    if raw is None:
        return {"status": "waiting"}

    np_arr = np.frombuffer(raw, np.uint8)
    img = cv2.imdecode(np_arr, cv2.IMREAD_GRAYSCALE)
    peak_memory = max(peak_memory, process.memory_info().rss)
    # ---- Aggregations ----
    mean_val = float(np.mean(img))
    std_val = float(np.std(img))
    histogram = np.histogram(img, bins=256, range=(0, 256))[0].tolist()
    peak_memory = max(peak_memory, process.memory_info().rss)
    # ---- Encode final image ----
    _, buffer = cv2.imencode(".png", img)
    peak_memory = max(peak_memory, process.memory_info().rss)
    img_b64 = base64.b64encode(buffer.tobytes()).decode()

    final_output = {
        "mean": mean_val,
        "std": std_val,
        "histogram": histogram,
        "image": img_b64
    }

    r.set(f"job:{job_id}:final", json.dumps(final_output))

    end_time = time.time()
    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user)
        + (cpu_end.system - cpu_start.system)
    )
    record_metrics(
        job_id,
        arrival_time,
        start_time,
        end_time,
        cpu_total,
        peak_memory
    )

    return {
        "job_id": job_id,
        "status": "completed",
        "mean": mean_val,
        "std": std_val
    }


