from fastapi import FastAPI, Request
import requests
import time
import psutil
import json
import os
import redis
peak_memory = 0
app = FastAPI()

# ---------- Redis ----------
REDIS_HOST = os.getenv(
    "REDIS_HOST",
    "redis.default.svc.cluster.local"
)

r = redis.Redis(
    host=REDIS_HOST,
    port=6379,
    decode_responses=True
)

# ---------- F3 Filter ----------
F3_URL = os.getenv(
    "F3_URL",
    "http://iot-f3-filter.default.svc.cluster.local"
)


# ---------- metrics ----------
def record_metrics(job_id, device_id, arrival, start, end, cpu_total,peak_memory):


    metrics = {
        "job_id": job_id,
        "device_id": device_id,
        "function": "iot-f2-decode",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(
        f"job:{job_id}:iot-f2-decode:{device_id}:{int(time.time()*1000)}",
        json.dumps(metrics)
    )

    print(json.dumps(metrics))


@app.post("/")
async def decode(request: Request):

    global peak_memory
    peak_memory = 0
    arrival = time.time()

    data = await request.json()

    required = ["job_id", "device_id", "timestamp", "value"]

    for k in required:
        if k not in data:
            return {"error": f"missing {k}"}

    job_id = data["job_id"]
    device_id = str(data["device_id"])

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()
    peak_memory = process.memory_info().rss
    # ---------- normalization ----------
    try:
        timestamp = int(data["timestamp"])
        value = float(data["value"])
    except:
        return {"error": "invalid data types"}

    # ---------- basic cleaning ----------
    # drop impossible values
    if value < -100 or value > 200:
        return {"status": "dropped_out_of_range"}

    # normalize (example)
    normalized_value = round(value, 2)
    peak_memory = max(peak_memory, process.memory_info().rss)
    payload = {
        "job_id": job_id,
        "device_id": device_id,
        "timestamp": timestamp,
        "value": normalized_value
    }

    # ---------- forward to F3 ----------
    try:
        requests.post(F3_URL, json=payload, timeout=3)
    except Exception as e:
        print("F3 dispatch failed:", e)

    end = time.time()

    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user) +
        (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, device_id, arrival, start, end, cpu_total, peak_memory)

    return {
        "status": "decoded",
        "device_id": device_id
    }
