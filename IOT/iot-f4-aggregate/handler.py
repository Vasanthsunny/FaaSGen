from fastapi import FastAPI, Request
import redis
import json
import time
import psutil
import os
import requests
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

# ---------- F5 Dashboard ----------
F5_URL = os.getenv(
    "F5_URL",
    "http://iot-f5-dashboard.default.svc.cluster.local"
)

# ---------- window size ----------
WINDOW_SIZE = 10  # seconds


# ---------- metrics ----------
def record_metrics(job_id, device_id, arrival, start, end, cpu_total, peak_memory):

   
    metrics = {
        "job_id": job_id,
        "device_id": device_id,
        "function": "iot-f4-aggregate",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)    
    }

    r.set(
        f"job:{job_id}:iot-f4-aggregate:{device_id}:{int(time.time()*1000)}",
        json.dumps(metrics)
    )

    print(json.dumps(metrics))


@app.post("/")
async def aggregate(request: Request):
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
    anomaly = data.get("anomaly", False)

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()
    peak_memory = process.memory_info().rss
    key = f"device:{device_id}:window"

    # ---------- load existing window ----------
    window_data = r.get(key)
    peak_memory = max(peak_memory, process.memory_info().rss)
    if window_data:
        window = json.loads(window_data)
    else:
        window = []

    # ---------- add new value ----------
    window.append((timestamp, value))
    peak_memory = max(peak_memory, process.memory_info().rss)
    # ---------- remove old values ----------
    cutoff = timestamp - WINDOW_SIZE

    window = [
        (ts, val) for (ts, val) in window
        if ts >= cutoff
    ]

    # ---------- compute stats ----------
    values = [val for (_, val) in window]
    peak_memory = max(peak_memory, process.memory_info().rss)
    if values:
        avg_val = sum(values) / len(values)
        min_val = min(values)
        max_val = max(values)
        count = len(values)
    else:
        avg_val = min_val = max_val = count = 0
    peak_memory = max(peak_memory, process.memory_info().rss)
    # ---------- store updated window ----------
    r.set(key, json.dumps(window))

    stats = {
        "job_id": job_id,
        "device_id": device_id,
        "timestamp": timestamp,
        "avg": round(avg_val, 2),
        "min": min_val,
        "max": max_val,
        "count": count,
        "anomaly": anomaly
    }

    # ---------- forward to F5 ----------
    try:
        requests.post(F5_URL, json=stats, timeout=3)
    except Exception as e:
        print("F5 dispatch failed:", e)

    end = time.time()

    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user) +
        (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, device_id, arrival, start, end, cpu_total, peak_memory)

    return {
        "status": "aggregated",
        "device_id": device_id,
        "avg": stats["avg"]
    }
