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

F6_URL = "http://log-f6-error-detect.default.svc.cluster.local"

def record_metrics(job_id, arrival, start, end, cpu_total, peak_memory):
    r.set(
        f"job:{job_id}:log-f4-extract",
        json.dumps({
            "job_id": job_id,
            "function": "log-f4-extract",
            "arrival_time": arrival,
            "start_time": start,
            "end_time": end,
            "processing_time_ms": (end-start)*1000,
            "cpu_time_sec": cpu_total,
            "memory_mb": peak_memory / (1024 * 1024)
        })
    )

@app.post("/")
async def extract(request: Request):

    global peak_memory
    peak_memory = 0
    arrival = time.time()
    data = await request.json()
    job_id = data["job_id"]
    shard_id = data["shard_id"]

    shard_data = r.get(f"job:{job_id}:shard:{shard_id}")
    if not shard_data:
        return {"status": "shard_not_ready"}

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()
    peak_memory = process.memory_info().rss
    lines = json.loads(shard_data)

    extracted = []
    peak_memory = max(peak_memory, process.memory_info().rss)
    for line in lines:
        parts = line.split()
        if len(parts) < 9:
            continue

        try:
            extracted.append({
                "ip": parts[0],
                "method": parts[5].replace('"', ''),
                "path": parts[6],
                "status": int(parts[8]),
                "size": int(parts[9]) if parts[9].isdigit() else 0
            })
        except:
            continue
    peak_memory = max(peak_memory, process.memory_info().rss)
    r.set(
        f"job:{job_id}:extract:{shard_id}",
        json.dumps(extracted)
    )

    end = time.time()
    cpu_end = process.cpu_times()

    cpu_total = (cpu_end.user-cpu_start.user)+(cpu_end.system-cpu_start.system)

    record_metrics(job_id, arrival, start, end, cpu_total, peak_memory)

    # Trigger F6
    try:
        requests.post(
            F6_URL,
            json={"job_id": job_id, "shard_id": shard_id},
            timeout=5
        )
    except:
        pass

    return {
        "job_id": job_id,
        "shard_id": shard_id,
        "status": "extracted"
    }
