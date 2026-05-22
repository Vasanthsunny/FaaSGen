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

def record_metrics(job_id, arrival, start, end, cpu_total, peak_memory):
    r.set(
        f"job:{job_id}:log-f8-publish-metrics",
        json.dumps({
            "job_id": job_id,
            "function": "log-f8-publish-metrics",
            "arrival_time": arrival,
            "start_time": start,
            "end_time": end,
            "processing_time_ms": (end-start)*1000,
            "cpu_time_sec": cpu_total,
            "memory_mb": peak_memory / (1024 * 1024)
        })
    )

@app.post("/")
async def publish(request: Request):

    global peak_memory
    peak_memory = 0
    arrival = time.time()
    data = await request.json()
    job_id = data["job_id"]

    global_data = r.get(f"job:{job_id}:global")
    if not global_data:
        return {"status": "no_global_data"}

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()
    peak_memory = process.memory_info().rss
    result = json.loads(global_data)

    # Simulated publishing (could push to DB / monitoring system)
    print("FINAL METRICS:", result)

    end = time.time()
    cpu_end = process.cpu_times()

    cpu_total = (cpu_end.user-cpu_start.user)+(cpu_end.system-cpu_start.system)
    peak_memory = max(peak_memory, process.memory_info().rss)

    record_metrics(job_id, arrival, start, end, cpu_total, peak_memory)

    return {
        "job_id": job_id,
        "status": "published",
        "result": result
    }
