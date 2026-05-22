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
import io
import requests
import uvicorn

app = FastAPI()
peak_memory = 0
# Redis
r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=False
)

# Next step
F2_URL = "http://etl-f2-clean.default.svc.cluster.local"


def serialize_df(df):
    return base64.b64encode(
        zlib.compress(
            pickle.dumps(df)
        )
    ).decode()


def record_metrics(job_id, arrival, start, end, cpu_total, peak_memory_mb):
    metrics = {
        "job_id": job_id,
        "function": "etl-f1-acquire",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory_mb
    }
    r.set(f"job:{job_id}:etl-f1-acquire", json.dumps(metrics))


@app.post("/")
async def acquire(request: Request):

    global peak_memory
    peak_memory = 0  # reset peak memory for this request

    arrival_time = time.time()
    job_id = str(uuid.uuid4())

    data = await request.json()

    filename = data["filename"]
    file_b64 = data["file"]

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start_time = time.time()
    peak_memory = process.memory_info().rss  
    # ---- Decode CSV ----
    decoded = base64.b64decode(file_b64)
    df = pd.read_csv(io.BytesIO(decoded))

    # ---- Profiling ----
    row_count = len(df)
    col_count = len(df.columns)

    # ---- Serialize & compress ----
    serialized = serialize_df(df)
    r.set(f"job:{job_id}:data", serialized)

    end_time = time.time()
    cpu_end = process.cpu_times()
    peak_memory = max(peak_memory, process.memory_info().rss)
    cpu_total = (
        (cpu_end.user - cpu_start.user)
        + (cpu_end.system - cpu_start.system)
    )

    peak_memory_mb = peak_memory / (1024 * 1024)

    record_metrics(job_id, arrival_time, start_time, end_time, cpu_total, peak_memory_mb)

    # Trigger F2
    try:
        resp = requests.post(F2_URL, json={"job_id": job_id}, timeout=10)
        print("F2 status:", resp.status_code)
        print("F2 response:", resp.text)
    except Exception as e:
        print("F2 ERROR:", str(e))

    return {
        "job_id": job_id,
        "filename": filename,
        "rows": row_count,
        "columns": col_count,
        "status": "acquired"
    }

