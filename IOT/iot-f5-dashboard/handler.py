from fastapi import FastAPI, Request
import redis
import json
import time
import psutil
import os
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


# ---------- metrics ----------
def record_metrics(job_id, device_id, arrival, start, end, cpu_total,peak_memory):

   
    metrics = {
        "job_id": job_id,
        "device_id": device_id,
        "function": "iot-f5-dashboard",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(
        f"job:{job_id}:iot-f5-dashboard:{device_id}:{int(time.time()*1000)}",
        json.dumps(metrics)
    )

    print(json.dumps(metrics))


@app.post("/")
async def dashboard(request: Request):
    global peak_memory
    peak_memory = 0
    arrival = time.time()

    data = await request.json()

    required = [
        "job_id", "device_id", "timestamp",
        "avg", "min", "max", "count"
    ]
    
    for k in required:
        if k not in data:
            return {"error": f"missing {k}"}
    
    job_id = data["job_id"]
    device_id = data["device_id"]
    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()
    peak_memory = process.memory_info().rss
    # ---------- print output ----------
    print("\n=== IoT Dashboard Update ===")
    print(f"Device: {device_id}")
    print(f"Time: {data['timestamp']}")
    print(f"Avg: {data['avg']}")
    print(f"Min: {data['min']}")
    print(f"Max: {data['max']}")
    print(f"Count: {data['count']}")
    print(f"Anomaly: {data.get('anomaly', False)}")
    print("============================\n")

    # ---------- store latest stats ----------
    r.set(
        f"device:{device_id}:latest",
        json.dumps(data)
    )

    end = time.time()

    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user) +
        (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, device_id, arrival, start, end, cpu_total, peak_memory)

    return {
        "status": "dashboard_updated",
        "device_id": device_id
    }
