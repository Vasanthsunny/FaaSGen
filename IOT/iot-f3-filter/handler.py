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

# ---------- F4 Aggregator ----------
F4_URL = os.getenv(
    "F4_URL",
    "http://iot-f4-aggregate.default.svc.cluster.local"
)


# ---------- metrics ----------
def record_metrics(job_id, device_id, arrival, start, end, cpu_total,peak_memory):

    

    metrics = {
        "job_id": job_id,
        "device_id": device_id,
        "function": "iot-f3-filter",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(
        f"job:{job_id}:iot-f3-filter:{device_id}:{int(time.time()*1000)}",
        json.dumps(metrics)
    )

    print(json.dumps(metrics))


@app.post("/")
async def filter_event(request: Request):


    global peak_memory
    peak_memory = 0
    arrival = time.time()

    data = await request.json()

    required = ["job_id", "device_id", "timestamp", "value"]

    for k in required:
        if k not in data:
            return {"error": f"missing {k}"}

    job_id = data["job_id"]
    device_id = data["device_id"]
    timestamp = int(data["timestamp"])
    value = float(data["value"])

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()
    peak_memory = process.memory_info().rss
    # ---------- filtering rules ----------

    # 1. Drop zero / invalid readings
    if value == 0:
        return {"status": "dropped_zero"}
    peak_memory = max(peak_memory, process.memory_info().rss)
    # 2. Drop extreme outliers
    if value < -50 or value > 150:
        return {"status": "dropped_outlier"}
    peak_memory = max(peak_memory, process.memory_info().rss)
    # 3. Simple anomaly detection (spike)
    prev_key = f"device:{device_id}:last_value"
    prev_val = r.get(prev_key)
    peak_memory = max(peak_memory, process.memory_info().rss)
    anomaly = False

    if prev_val is not None:
        prev_val = float(prev_val)

        # spike detection
        if abs(value - prev_val) > 20:
            anomaly = True
    peak_memory = max(peak_memory, process.memory_info().rss)
    # update last value
    r.set(prev_key, value)

    payload = {
        "job_id": job_id,
        "device_id": device_id,
        "timestamp": timestamp,
        "value": value,
        "anomaly": anomaly
    }

    # ---------- forward to F4 ----------
    try:
        requests.post(F4_URL, json=payload, timeout=3)
    except Exception as e:
        print("F4 dispatch failed:", e)

    end = time.time()

    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user) +
        (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, device_id, arrival, start, end, cpu_total, peak_memory)

    return {
        "status": "filtered",
        "device_id": device_id,
        "anomaly": anomaly
    }
