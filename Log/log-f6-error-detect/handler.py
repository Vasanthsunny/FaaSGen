from fastapi import FastAPI, Request
import redis
import json
import time
import psutil
import requests
import uvicorn

peak_memory = 0
app = FastAPI()

r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=True
)

F7_URL = "http://log-f7-global-aggregate.default.svc.cluster.local"

def record_metrics(job_id, arrival, start, end, cpu_total, peak_memory):
    r.set(
        f"job:{job_id}:log-f6-error-detect",
        json.dumps({
            "job_id": job_id,
            "function": "log-f6-error-detect",
            "arrival_time": arrival,
            "start_time": start,
            "end_time": end,
            "processing_time_ms": (end-start)*1000,
            "cpu_time_sec": cpu_total,
            "memory_mb": peak_memory / (1024 * 1024)
        })
    )

@app.post("/")
async def detect(request: Request):

    global peak_memory
    peak_memory = 0
    arrival = time.time()
    data = await request.json()
    job_id = data["job_id"]
    shard_id = data["shard_id"]

    extract_data = r.get(f"job:{job_id}:extract:{shard_id}")
    if not extract_data:
        return {"status": "extract_not_ready"}

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()
    peak_memory = process.memory_info().rss
    logs = json.loads(extract_data)

    total_requests = len(logs)
    error_4xx = 0
    error_5xx = 0
    peak_memory = max(peak_memory, process.memory_info().rss)
    for entry in logs:
        status = entry.get("status", 0)
        if 400 <= status < 500:
            error_4xx += 1
        elif 500 <= status < 600:
            error_5xx += 1
    peak_memory = max(peak_memory, process.memory_info().rss)
    error_rate = (
        (error_4xx + error_5xx) / total_requests
        if total_requests else 0
    )
    peak_memory = max(peak_memory, process.memory_info().rss)
    result = {
        "total_requests": total_requests,
        "error_rate": error_rate
    }

    r.set(
        f"job:{job_id}:error:{shard_id}",
        json.dumps(result)
    )

    end = time.time()
    cpu_end = process.cpu_times()

    cpu_total = (cpu_end.user-cpu_start.user)+(cpu_end.system-cpu_start.system)
    

    record_metrics(job_id, arrival, start, end, cpu_total, peak_memory)

    # Trigger F7
    try:
        requests.post(
            F7_URL,
            json={"job_id": job_id},
            timeout=5
        )
    except:
        pass

    return {
        "job_id": job_id,
        "shard_id": shard_id,
        "status": "error_checked"
    }

