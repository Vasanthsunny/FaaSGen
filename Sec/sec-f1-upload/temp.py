from fastapi import FastAPI, Request
import redis
import json
import time
import psutil
import os
import uuid
import base64
import requests
import io
from minio import Minio

app = FastAPI()

# ---------- Redis ----------
REDIS_HOST = os.getenv(
    "REDIS_HOST",
    "redis.default.svc.cluster.local"
)

r = redis.Redis(
    host=REDIS_HOST,
    port=6379,
    decode_responses=True
)

# ---------- MinIO ----------
MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "minio.default.svc.cluster.local:9000"
)

MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "password123")

BUCKET = "sec-artifacts"

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS,
    secret_key=MINIO_SECRET,
    secure=False
)

# ---------- F2 ----------
F2_URL = os.getenv(
    "F2_URL",
    "http://sec-f2-unpack.default.svc.cluster.local"
)

MAX_SIZE_MB = 50


# ---------- metrics ----------
def record_metrics(job_id, arrival, start, end, cpu_total, size_mb):

    p = psutil.Process()

    metrics = {
        "job_id": job_id,
        "function": "sec-f1-upload",
        "artifact_size_mb": size_mb,
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": p.memory_info().rss / (1024 * 1024)
    }

    r.set(f"job:{job_id}:sec-f1-upload", json.dumps(metrics))
    print(json.dumps(metrics))


@app.post("/")
async def upload_artifact(request: Request):

    arrival = time.time()

    data = await request.json()

    if "artifact_base64" not in data:
        return {"error": "missing artifact_base64"}

    job_id = str(uuid.uuid4())

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()

    # ---------- decode ----------
    try:
        artifact_bytes = base64.b64decode(data["artifact_base64"])
    except Exception:
        return {"error": "invalid base64"}

    size_mb = len(artifact_bytes) / (1024 * 1024)

    # ---------- validate ----------
    if size_mb > MAX_SIZE_MB:
        return {"error": f"artifact too large ({size_mb:.2f} MB)"}

    object_name = f"{job_id}.zip"

    # ---------- upload to MinIO ----------
    try:
        minio_client.put_object(
            BUCKET,
            object_name,
            io.BytesIO(artifact_bytes),
            length=len(artifact_bytes),
            content_type="application/zip"
        )
    except Exception as e:
        return {"error": f"minio upload failed: {str(e)}"}

    # ---------- store pointer in Redis ----------
    r.set(
        f"job:{job_id}:artifact_path",
        object_name
    )

    # ---------- trigger F2 ----------
    try:
        requests.post(
            F2_URL,
            json={"job_id": job_id},
            timeout=5
        )
    except Exception as e:
        print("F2 trigger failed:", e)

    end = time.time()

    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user) +
        (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, arrival, start, end, cpu_total, size_mb)

    return {
        "job_id": job_id,
        "object": object_name,
        "status": "uploaded_to_minio"
    }
