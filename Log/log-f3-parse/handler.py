from fastapi import FastAPI, Request
import redis
import json
import time
import psutil
import requests
import re
import uvicorn
peak_memory = 0
app = FastAPI()

r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=True
)

F5_URL = "http://log-f5-sessionize.default.svc.cluster.local"

LOG_PATTERN = re.compile(
    r'(?P<ip>\S+) \S+ \S+ \[(?P<time>[^\]]+)\] '
    r'"(?P<method>\S+) (?P<path>\S+) \S+" '
    r'(?P<status>\d+) (?P<size>\d+)'
)

def record_metrics(job_id, arrival, start, end, cpu_total, peak_memory):
    r.set(
        f"job:{job_id}:log-f3-parse",
        json.dumps({
            "job_id": job_id,
            "function": "log-f3-parse",
            "arrival_time": arrival,
            "start_time": start,
            "end_time": end,
            "processing_time_ms": (end-start)*1000,
            "cpu_time_sec": cpu_total,
            "memory_mb": peak_memory / (1024 * 1024)
        })
    )

@app.post("/")
async def parse(request: Request):

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

    parsed = []
    for line in lines:
        m = LOG_PATTERN.match(line)
        if m:
            parsed.append(m.groupdict())
    peak_memory = max(peak_memory, process.memory_info().rss)
    r.set(
        f"job:{job_id}:parsed:{shard_id}",
        json.dumps(parsed)
    )

    end = time.time()
    cpu_end = process.cpu_times()

    cpu_total = (cpu_end.user-cpu_start.user)+(cpu_end.system-cpu_start.system)
    
    record_metrics(job_id, arrival, start, end, cpu_total, peak_memory)

    # Trigger F5
    try:
        requests.post(
            F5_URL,
            json={"job_id": job_id, "shard_id": shard_id},
            timeout=5
        )
    except:
        pass

    return {
        "job_id": job_id,
        "shard_id": shard_id,
        "status": "parsed"
    }


