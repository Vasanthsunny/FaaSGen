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

F3_URL = "http://log-f3-parse.default.svc.cluster.local"
F4_URL = "http://log-f4-extract.default.svc.cluster.local"


def record_metrics(job_id, arrival, start, end, cpu_total, peak_memory):
    metrics = {
        "job_id": job_id,
        "function": "log-f2-shard",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(
        f"job:{job_id}:log-f2-shard",
        json.dumps(metrics)
    )


@app.post("/")
async def shard(request: Request):

    global peak_memory
    peak_memory = 0
    arrival_time = time.time()
    data = await request.json()
    job_id = data["job_id"]

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start_time = time.time()
    peak_memory = process.memory_info().rss
    # ---- Load Raw Logs ----
    raw_data = r.get(f"job:{job_id}:raw_logs")
    if not raw_data:
        return {"status": "no_raw_logs"}

    lines = json.loads(raw_data)
    peak_memory = max(peak_memory, process.memory_info().rss)
    total_lines = len(lines)
    shard_size = total_lines // SHARD_COUNT

    for shard_id in range(SHARD_COUNT):

        start = shard_id * shard_size
        end = total_lines if shard_id == SHARD_COUNT - 1 else (shard_id + 1) * shard_size

        shard_lines = lines[start:end]

        # Store shard as JSON
        r.set(
            f"job:{job_id}:shard:{shard_id}",
            json.dumps(shard_lines)
        )

        payload = {
            "job_id": job_id,
            "shard_id": shard_id
        }

        # Level 1 Parallel
        try:
            requests.post(F3_URL, json=payload, timeout=5)
            requests.post(F4_URL, json=payload, timeout=5)
        except Exception as e:
            print("Trigger error:", str(e))
    peak_memory = max(peak_memory, process.memory_info().rss)
    end_time = time.time()
    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user) +
        (cpu_end.system - cpu_start.system)
    )

    
    record_metrics(job_id, arrival_time, start_time, end_time, cpu_total, peak_memory)

    return {
        "job_id": job_id,
        "status": "sharded",
        "total_lines": total_lines
    }

