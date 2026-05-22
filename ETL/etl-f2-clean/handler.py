from fastapi import FastAPI, Request
import pandas as pd
import uuid
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

F3_URL = "http://etl-f3-transform.default.svc.cluster.local"


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
        "function": "etl-f2-clean",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory_mb
    }
    r.set(f"job:{job_id}:etl-f2-clean", json.dumps(metrics))


@app.post("/")
async def clean(request: Request):

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

    # ---- Cleaning operations ----
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    peak_memory = max(peak_memory, process.memory_info().rss)
    df = df.dropna()
    df = df.drop_duplicates()

    # Strip whitespace in object columns
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    # Normalize region values
    if "region" in df.columns:
        df["region"] = df["region"].str.lower()
    peak_memory = max(peak_memory, process.memory_info().rss)
    serialized = serialize_df(df)
    r.set(f"job:{job_id}:data", serialized)

    end_time = time.time()
    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user)
        + (cpu_end.system - cpu_start.system)
    )

    peak_memory = max(peak_memory, process.memory_info().rss)
    peak_memory_mb = peak_memory / (1024 * 1024)

    record_metrics(job_id, arrival_time, start_time, end_time, cpu_total, peak_memory_mb)

    # Trigger F3
    try:
        requests.post(F3_URL, json={"job_id": job_id})
    except:
        pass

    return {
        "job_id": job_id,
        "status": "cleaned"
    }

