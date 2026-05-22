from fastapi import FastAPI, Request
import pandas as pd
import time
import psutil
import redis
import json
import pickle
import zlib
import base64
import requests
import uvicorn
peak_memory = 0
app = FastAPI()

r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=False
)

F5_URL = "http://etl-f5-output.default.svc.cluster.local"


def deserialize_df(data):
    return pickle.loads(
        zlib.decompress(
            base64.b64decode(data)
        )
    )


def serialize_df(df):
    return base64.b64encode(
        zlib.compress(
            pickle.dumps(df)
        )
    ).decode()


def record_metrics(job_id, arrival, start, end, cpu_total, peak_memory_mb):
    metrics = {
        "job_id": job_id,
        "function": "etl-f4-aggregate",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory_mb
    }
    r.set(f"job:{job_id}:etl-f4-aggregate", json.dumps(metrics))


@app.post("/")
async def aggregate(request: Request):

    global peak_memory
    peak_memory = 0  # reset peak memory for this request

    arrival_time = time.time()

    data = await request.json()
    job_id = data["job_id"]

    df_serialized = r.get(f"job:{job_id}:data")
    if not df_serialized:
        return {"error": "Data not found"}

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start_time = time.time()
    peak_memory = process.memory_info().rss
    df = deserialize_df(df_serialized)

    # ---- Aggregations ----

    # Revenue by region
    region_summary = (
        df.groupby("region")
        .agg(
            total_revenue=("net_amount", "sum"),
            total_transactions=("transaction_id", "count"),
            avg_order_value=("net_amount", "mean")
        )
        .reset_index()
        .sort_values(by="total_revenue", ascending=False)
    )
    peak_memory = max(peak_memory, process.memory_info().rss)
    # Revenue by month
    monthly_summary = (
        df.groupby("month")
        .agg(
            monthly_revenue=("net_amount", "sum"),
            transactions=("transaction_id", "count")
        )
        .reset_index()
        .sort_values(by="month")
    )
    peak_memory = max(peak_memory, process.memory_info().rss)
    # Store aggregated results separately
    r.set(f"job:{job_id}:region_summary", serialize_df(region_summary))
    r.set(f"job:{job_id}:monthly_summary", serialize_df(monthly_summary))

    end_time = time.time()
    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user)
        + (cpu_end.system - cpu_start.system)
    )

    peak_memory = max(peak_memory, process.memory_info().rss)
    peak_memory_mb = peak_memory / (1024 * 1024)

    record_metrics(job_id, arrival_time, start_time, end_time, cpu_total, peak_memory_mb)

    # Trigger F5
    try:
        requests.post(F5_URL, json={"job_id": job_id})
    except:
        pass

    return {
        "job_id": job_id,
        "status": "aggregated"
    }
