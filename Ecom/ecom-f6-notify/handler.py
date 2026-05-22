from fastapi import FastAPI, Request
import redis
import time
import psutil
import json
import random
import uvicorn

app = FastAPI()

r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=True
)

def record_metrics(job_id, arrival, start, end, cpu_total):
    process = psutil.Process()
    mem = process.memory_info().rss / (1024 * 1024)

    metrics = {
        "job_id": job_id,
        "function": "ecom-f6-notify",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end-start)*1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": mem
    }

    r.set(f"job:{job_id}:ecom-f6-notify", json.dumps(metrics))


@app.post("/")
async def notify(request: Request):

    arrival = time.time()
    data = await request.json()
    job_id = data["job_id"]

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()

    approval_raw = r.get(f"job:{job_id}:approved")
    if not approval_raw:
        return {"status": "waiting"}

    approval = json.loads(approval_raw)

    # Simulate sending email/SMS latency
    time.sleep(0.3)

    final_status = "order_confirmed" if approval["approved"] else "order_failed"

    r.set(f"job:{job_id}:final_status",
          json.dumps({"status": final_status}))

    end = time.time()
    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user)
        + (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, arrival, start, end, cpu_total)

    return {"job_id": job_id, "final_status": final_status}

