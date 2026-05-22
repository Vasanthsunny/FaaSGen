from fastapi import FastAPI, Request
import redis
import json
import time
import psutil
import requests
import uvicorn

peak_memory = 0
app = FastAPI()

SHARD_COUNT = 4

r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=True
)

F8_URL = "http://log-f8-publish-metrics.default.svc.cluster.local"

def record_metrics(job_id, arrival, start, end, cpu_total, peak_memory):
    r.set(
        f"job:{job_id}:log-f7-global-aggregate",
        json.dumps({
            "job_id": job_id,
            "function": "log-f7-global-aggregate",
            "arrival_time": arrival,
            "start_time": start,
            "end_time": end,
            "processing_time_ms": (end-start)*1000,
            "cpu_time_sec": cpu_total,
            "memory_mb": peak_memory / (1024 * 1024)
        })
    )

@app.post("/")
async def aggregate(request: Request):
    global peak_memory
    peak_memory = 0
    arrival = time.time()
    data = await request.json()
    job_id = data["job_id"]

    # ---- Barrier Check ----
    for shard_id in range(SHARD_COUNT):
        if not r.get(f"job:{job_id}:session:{shard_id}"):
            return {"status": "waiting_sessions"}
        if not r.get(f"job:{job_id}:error:{shard_id}"):
            return {"status": "waiting_errors"}

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()
    peak_memory = max(peak_memory, process.memory_info().rss)
    total_sessions = 0
    total_requests = 0
    weighted_error_sum = 0

    for shard_id in range(SHARD_COUNT):

        session_data = json.loads(
            r.get(f"job:{job_id}:session:{shard_id}")
        )

        error_data = json.loads(
            r.get(f"job:{job_id}:error:{shard_id}")
        )

        total_sessions += session_data["total_sessions"]

        shard_requests = error_data["total_requests"]
        total_requests += shard_requests

        weighted_error_sum += (
            error_data["error_rate"] * shard_requests
        )
    peak_memory = max(peak_memory, process.memory_info().rss)
    global_error_rate = (
        weighted_error_sum / total_requests
        if total_requests else 0
    )
    peak_memory = max(peak_memory, process.memory_info().rss)
    final_result = {
        "total_sessions": total_sessions,
        "total_requests": total_requests,
        "error_rate": global_error_rate
    }

    r.set(
        f"job:{job_id}:global",
        json.dumps(final_result)
    )

    end = time.time()
    cpu_end = process.cpu_times()

    cpu_total = (cpu_end.user-cpu_start.user)+(cpu_end.system-cpu_start.system)
    
    record_metrics(job_id, arrival, start, end, cpu_total, peak_memory)

    # ---- Trigger F8 ----
    try:
        requests.post(
            F8_URL,
            json={"job_id": job_id},
            timeout=5
        )
    except:
        pass

    return {
        "job_id": job_id,
        "status": "aggregated"
    }
