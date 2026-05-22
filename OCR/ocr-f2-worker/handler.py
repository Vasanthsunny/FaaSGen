from fastapi import FastAPI, Request
import redis
import json
import time
import psutil
import requests
import cv2
import numpy as np
import pytesseract
import uvicorn

app = FastAPI()
F3_URL = "http://ocr-f3-merge.default.svc.cluster.local"

peak_memory = 0

r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=False
)


def record_metrics(job_id, arrival, start, end, cpu_total, peak_memory):

    metrics = {
        "job_id": job_id,
        "function": "ocr-f2-worker",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end-start)*1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024*1024)
    }

    r.set(
        f"job:{job_id}:ocr-f2-worker:{int(start)}",
        json.dumps(metrics)
    )


@app.post("/")
async def ocr(request: Request):
    global peak_memory
    peak_memory = 0  # reset peak memory for this request

    arrival = time.time()

    data = await request.json()

    job_id = data["job_id"]
    page_id = data["page_id"]

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()

    img_bytes = r.get(f"job:{job_id}:page:{page_id}")
    if img_bytes is None:
        return {"error" : "page img not found"}

    img_array = np.frombuffer(img_bytes, np.uint8)

    peak_memory = process.memory_info().rss  # memory after loading image
    
    img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)

    peak_memory = max(peak_memory, process.memory_info().rss)  # memory after decoding image

    if img is None:
        return {"error":"decode failed"}

    text = pytesseract.image_to_string(img)
    peak_memory = max(peak_memory, process.memory_info().rss)  # memory after OCR
    r.set(
        f"job:{job_id}:ocr:{page_id}",
        json.dumps({"text": text})
    )
    r.delete(f"job:{job_id}:page:{page_id}")

    # trigger merge stage
    try:
        requests.post(
            F3_URL,
            json={"job_id": job_id},
            timeout=30
         )
    except Exception as e:
        print("F3 trigger failed:", e)
    
    end = time.time()

    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user-cpu_start.user) +
        (cpu_end.system-cpu_start.system)
    )

    record_metrics(job_id, arrival, start, end, cpu_total, peak_memory)

    return {
        "job_id": job_id,
        "page_id": page_id,
        "status": "ocr_complete"
    }

