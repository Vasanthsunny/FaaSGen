from fastapi import FastAPI, Request
import uuid
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

# ---------- F2 Decode ----------
F2_URL = os.getenv(
    "F2_URL",
    "http://iot-f2-decode.default.svc.cluster.local"
)


# ---------- metrics ----------
def record_metrics(job_id, arrival, start, end, cpu_total, batch_size, peak_memory):

    
    metrics = {
        "job_id": job_id,
        "function": "iot-f1-ingest",
        "batch_size": batch_size,
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(f"job:{job_id}:iot-f1-ingest", json.dumps(metrics))
    print(json.dumps(metrics))


@app.post("/")
async def ingest(request: Request):

    global peak_memory
    peak_memory = 0
    arrival = time.time()
    data = await request.json()

    if "batch" not in data:
        return {"error": "missing batch"}

    batch = data["batch"]

    if not isinstance(batch, list) or len(batch) == 0:
        return {"error": "batch must be non-empty list"}

    job_id = str(uuid.uuid4())
    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()
    peak_memory = process.memory_info().rss
    session = requests.Session()

    # ---------- dispatch each sensor record ----------
    for record in batch:

        if not all(k in record for k in ["device_id", "timestamp", "value"]):
            continue  # skip invalid

        payload = {
            "job_id": job_id,
            "device_id": record["device_id"],
            "timestamp": record["timestamp"],
            "value": record["value"]
        }
        
        try:
            session.post(F2_URL, json=payload, timeout=3)
        except Exception as e:
            print("F2 dispatch failed:", e)
    peak_memory = max(peak_memory, process.memory_info().rss)
    end = time.time()

    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user) +
        (cpu_end.system - cpu_start.system)
    )

    record_metrics(
        job_id,
        arrival,
        start,
        end,
        cpu_total,
        len(batch),
        peak_memory
    )

    return {
        "job_id": job_id,
        "records": len(batch),
        "status": "dispatched"
    }
