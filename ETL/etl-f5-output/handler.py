from fastapi import FastAPI, Request
import pandas as pd
import time
import psutil
import redis
import json
import pickle
import zlib
import base64
import uvicorn
peak_memory = 0
app = FastAPI()

r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=False
)


def deserialize_df(data):
    return pickle.loads(
        zlib.decompress(
            base64.b64decode(data)
        )
    )


def record_metrics(job_id, arrival, start, end, cpu_total, peak_memory_mb):
    metrics = {
        "job_id": job_id,
        "function": "etl-f5-output",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory_mb
    }
    r.set(f"job:{job_id}:etl-f5-output", json.dumps(metrics))


@app.post("/")
async def finalize(request: Request):
    global peak_memory  
    peak_memory = 0  # reset peak memory for this request
    arrival_time = time.time()

    data = await request.json()
    job_id = data["job_id"]

    region_data = r.get(f"job:{job_id}:region_summary")
    monthly_data = r.get(f"job:{job_id}:monthly_summary")

    if not region_data or not monthly_data:
        return {"status": "waiting"}

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start_time = time.time()
    peak_memory = process.memory_info().rss
    region_df = deserialize_df(region_data)
    monthly_df = deserialize_df(monthly_data)

    # ---- Final validation ----

    total_revenue = region_df["total_revenue"].sum()
    total_transactions = region_df["total_transactions"].sum()

    # Basic sanity checks
    validation_passed = True
    if total_revenue <= 0 or total_transactions <= 0:
        validation_passed = False
    peak_memory = max(peak_memory, process.memory_info().rss)
    pipeline_summary = {
        "job_id": job_id,
        "total_revenue": float(total_revenue),
        "total_transactions": int(total_transactions),
        "regions_processed": len(region_df),
        "months_processed": len(monthly_df),
        "validation_passed": validation_passed,
        "status": "completed"
    }
    peak_memory = max(peak_memory, process.memory_info().rss)
    r.set(f"job:{job_id}:final_summary", json.dumps(pipeline_summary))

    end_time = time.time()
    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user)
        + (cpu_end.system - cpu_start.system)
    )

    peak_memory = max(peak_memory, process.memory_info().rss)
    peak_memory_mb = peak_memory / (1024 * 1024)

    record_metrics(job_id, arrival_time, start_time, end_time, cpu_total, peak_memory_mb)

    return pipeline_summary

