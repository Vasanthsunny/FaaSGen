from fastapi import FastAPI, Request
import redis
import json
import time
import psutil
import os
import io
import zipfile
import tarfile
import shutil
from minio import Minio
import requests

app = FastAPI()

peak_memory = 0
# ---------- Redis ----------
REDIS_HOST = os.getenv("REDIS_HOST", "redis.default.svc.cluster.local")

r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

# ---------- MinIO ----------
MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "minio.default.svc.cluster.local:9000"
)

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key="admin",
    secret_key="password123",
    secure=False
)

BUCKET = "sec-artifacts"

# ---------- F3 ----------
SAST_URL = os.getenv("SAST_URL", "http://sec-f3-sast.default.svc.cluster.local")
DEP_URL = os.getenv("DEP_URL", "http://sec-f3-dep.default.svc.cluster.local")
DAST_URL = os.getenv("DAST_URL", "http://sec-f3-dast.default.svc.cluster.local")


def record_metrics(job_id, arrival, start, end, cpu_total, peak_memory, file_count):
  
    metrics = {
        "job_id": job_id,
        "function": "sec-f2-unpack",
        "files": file_count,
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory/(1024 * 1024)
    }

    r.set(f"job:{job_id}:sec-f2-unpack", json.dumps(metrics))
    print(json.dumps(metrics))


@app.post("/")
async def extract(request: Request):
    global peak_memory
    peak_memory = 0  # reset peak memory for this request
    arrival = time.time()
    data = await request.json()
    job_id = data["job_id"]

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()

    # ---------- get artifact path ----------
    object_name = r.get(f"job:{job_id}:artifact_path")

    if not object_name:
        return {"error": "artifact_path not found"}

    # ---------- download artifact ----------
    response = minio_client.get_object(BUCKET, object_name)
    artifact_bytes = response.read()

    # ---------- prepare temp dir ----------
    work_dir = f"/tmp/{job_id}"

    if os.path.exists(work_dir):
        shutil.rmtree(work_dir)

    os.makedirs(work_dir, exist_ok=True)

    # ---------- extract ----------
    extracted = False

    try:
        with zipfile.ZipFile(io.BytesIO(artifact_bytes)) as z:
            peak_memory = process.memory_info().rss  # memory after loading zip
            z.extractall(work_dir)
            peak_memory = max(peak_memory, process.memory_info().rss)  # update peak memory after extraction
            extracted = True
    except:
        pass

    if not extracted:
        try:
            with tarfile.open(fileobj=io.BytesIO(artifact_bytes)) as t:
                t.extractall(work_dir)
                peak_memory = max(peak_memory, process.memory_info().rss)  # update peak memory after extraction
                extracted = True
        except:
            return {"error": "unsupported archive"}

    # ---------- upload extracted files ----------
    file_list = []

    for root, _, files in os.walk(work_dir):
        for f in files:

            full_path = os.path.join(root, f)
            rel_path = os.path.relpath(full_path, work_dir)

            object_key = f"{job_id}/files/{rel_path}"

            try:
                with open(full_path, "rb") as file_data:
                    minio_client.put_object(
                        BUCKET,
                        object_key,
                        file_data,
                        length=os.path.getsize(full_path)
                    )
                peak_memory = max(peak_memory, process.memory_info().rss)  # update peak memory after each upload
            except Exception as e:
                print("upload failed:", e)
                continue

            file_list.append(object_key)

    # ---------- store file list ----------
    r.set(
        f"job:{job_id}:file_list",
        json.dumps(file_list)
    )

    payload = {
        "job_id": job_id
    }

    # ---------- trigger scanners ----------
    requests.post(SAST_URL, json=payload)
    requests.post(DEP_URL, json=payload)
    requests.post(DAST_URL, json=payload)

    end = time.time()

    cpu_end = process.cpu_times()
    cpu_total = (
        (cpu_end.user - cpu_start.user) +
        (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, arrival, start, end, cpu_total, peak_memory, len(file_list))

    return {
        "job_id": job_id,
        "files": len(file_list),
        "status": "extracted_and_uploaded"
    }