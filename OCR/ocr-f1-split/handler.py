from fastapi import FastAPI, Request
import redis
import json
import time
import psutil
import uuid
import base64
import requests
import io
import os
from pdf2image import convert_from_bytes
import uvicorn

app = FastAPI()
peak_memory = 0
REDIS_HOST = os.getenv("REDIS_HOST", "redis.default.svc.cluster.local")

r = redis.Redis(
    host=REDIS_HOST,
    port=6379,
    decode_responses=False
)

F2_URL = os.getenv(
    "F2_URL",
    "http://ocr-f2-worker.default.svc.cluster.local"
)


# ---------- metrics ----------
def record_metrics(job_id, arrival, start, end, cpu_total, peak_memory):

    metrics = {
        "job_id": job_id,
        "function": "ocr-f1-split",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(f"job:{job_id}:ocr-f1-split", json.dumps(metrics))


@app.post("/")
async def split(request: Request):
    global peak_memory
    peak_memory = 0  # reset peak memory for this request

    arrival = time.time()

    data = await request.json()


    job_id = str(uuid.uuid4())

    pdf_b64 = data["pdf"]

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()

    try:
        pdf_bytes = base64.b64decode(pdf_b64)
        peak_memory = process.memory_info().rss  # memory after decoding
    except Exception:
        return {"error": "invalid base64 pdf"}



    # ---------- convert pdf → images ----------
    try:
        pages = convert_from_bytes(pdf_bytes)
        peak_memory = max(peak_memory, process.memory_info().rss)  # memory after conversion
    except Exception as e:
        return {"error": f"pdf conversion failed: {str(e)}"}

    page_count = len(pages)

    r.set(f"job:{job_id}:page_count", page_count)

    # ---------- store pages + dispatch OCR ----------
    for i, page in enumerate(pages, start=1):

        buffer = io.BytesIO()
        peak_memory = max(peak_memory, process.memory_info().rss)  # memory after creating buffer
        page.save(buffer, format="JPEG", quality=85)

        r.set(f"job:{job_id}:page:{i}", buffer.getvalue())
        # trigger OCR worker
        try:
            requests.post(
                F2_URL,
                json={
                    "job_id": job_id,
                    "page_id": i
                },
                timeout=30
            )
        except Exception as e:
            print("F2 dispatch failed:", e)

    end = time.time()

    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user) +
        (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, arrival, start, end, cpu_total, peak_memory)

    return {
        "job_id": job_id,
        "pages": page_count,
        "status": "split_complete"
    }


