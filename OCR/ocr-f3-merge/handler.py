from fastapi import FastAPI, Request
import redis
import json
import time
import psutil
import requests
import os
import uvicorn

app = FastAPI()
peak_memory = 0
F4_URL = "http://ocr-f4-extract.default.svc.cluster.local"

REDIS_HOST = os.getenv("REDIS_HOST", "redis.default.svc.cluster.local")

r = redis.Redis(
    host=REDIS_HOST,
    port=6379,
    decode_responses=True
)

# ---------- metrics ----------
def record_metrics(job_id, arrival, start, end, cpu_total, peak_memory):
  
    metrics = {
        "job_id": job_id,
        "function": "ocr-f3-merge",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(f"job:{job_id}:ocr-f3-merge", json.dumps(metrics))


@app.post("/")
async def merge(request: Request):

    global peak_memory
    peak_memory = 0  # reset peak memory for this request
    arrival = time.time()

    data = await request.json()
    job_id = data["job_id"]

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()

    # ---------- lock ----------
    lock_key = f"job:{job_id}:merge_done"
    peak_memory = process.memory_info().rss  # memory after acquiring lock
    if r.get(lock_key):
        return {"status": "already_merged"}

    # ---------- read page count ----------
    page_count = r.get(f"job:{job_id}:page_count")
    peak_memory = max(peak_memory, process.memory_info().rss)  # memory after reading page count
    if not page_count:
        return {"error": "page_count not found"}

    page_count = int(page_count)

    pages = []

    # ---------- check OCR outputs ----------
    for i in range(1, page_count + 1):

        text = r.get(f"job:{job_id}:ocr:{i}")

        if text is None:
            return {"status": "waiting", "missing_page": i}

        text_obj = json.loads(text)
        pages.append(text_obj["text"])
        peak_memory = max(peak_memory, process.memory_info().rss)  # memory after reading OCR output
    # ---------- mark merged ----------
    r.set(lock_key, "1")

    # ---------- merge ----------
    document_text = "\n\n".join(pages)
    peak_memory = max(peak_memory, process.memory_info().rss)  # memory after merging text
    r.set(f"job:{job_id}:document_text", document_text)

    # ---------- cleanup ----------
    for i in range(1, page_count + 1):
        r.delete(f"job:{job_id}:ocr:{i}")
    peak_memory = max(peak_memory, process.memory_info().rss)  # memory after cleanup
    # ---------- trigger f4 ----------
    try:
        resp = requests.post(
            F4_URL,
            json={"job_id": job_id},
            timeout=30
        )
        print("F4 triggered:", resp.status_code)
    except Exception as e:
        print("F4 trigger failed:", e)

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
        "status": "merged"
    }