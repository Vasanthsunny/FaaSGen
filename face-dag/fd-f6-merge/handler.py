from fastapi import FastAPI, Request
import redis
import json
import time
import psutil
import uvicorn
peak_memory = 0
app = FastAPI()

r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=True
)


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
async def merge(request: Request):
    global peak_memory
    peak_memory = 0  # reset peak memory for this request
    arrival_time = time.time()

    data = await request.json()
    job_id = data["job_id"]

    process = psutil.Process()

    # ---- CPU START ----
    cpu_start = process.cpu_times()
    start_time = time.time()
    peak_memory = process.memory_info().rss
    logits = r.get(f"job:{job_id}:logits")
    bbox = r.get(f"job:{job_id}:bbox")
    landmarks = r.get(f"job:{job_id}:landmarks")
    peak_memory = max(peak_memory, process.memory_info().rss)
    # If not all ready → exit immediately
    if not (logits and bbox and landmarks):
        return {"job_id": job_id, "status": "waiting"}

    # Merge results
    final_result = {
        "logits": logits,
        "bbox": bbox,
        "landmarks": landmarks
    }
    peak_memory = max(peak_memory, process.memory_info().rss)
    r.set(f"job:{job_id}:final", json.dumps(final_result))

    # ---- CPU END ----
    end_time = time.time()
    cpu_end = process.cpu_times()

    cpu_user = cpu_end.user - cpu_start.user
    cpu_system = cpu_end.system - cpu_start.system
    cpu_total = cpu_user + cpu_system

    peak_memory = max(peak_memory, process.memory_info().rss)

    # ---- Store metrics ----
    record_metrics(
        job_id,
        "fd-f6-merge",
        arrival_time,
        start_time,
        end_time,
        cpu_total,
        peak_memory
    )

    return {"job_id": job_id, "status": "completed"}

