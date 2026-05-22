from fastapi import FastAPI, Request
import redis
import json
import time
import psutil
import os
import uuid
import qrcode
import io
import base64

app = FastAPI()
peak_memory = 0
# ---------- Redis ----------
REDIS_HOST = os.getenv(
    "REDIS_HOST",
    "redis.default.svc.cluster.local"
)

r = redis.Redis(
    host=REDIS_HOST,
    port=6379,
    decode_responses=False  # important for binary
)


# ---------- metrics ----------
def record_metrics(job_id, arrival, start, end, cpu_total, peak_memory):

    metrics = {
        "job_id": job_id,
        "function": "qr-f1-gen",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(
        f"job:{job_id}:qr-f1-gen",
        json.dumps(metrics)
    )

    print(json.dumps(metrics))


@app.post("/")
async def generate_qr(request: Request):
    global peak_memory
    peak_memory = 0  # reset peak memory for this request   
    arrival = time.time()

    data = await request.json()

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()

    if "data" not in data:
        return {"error": "missing 'data' field"}

    qr_data = data["data"]

    job_id = str(uuid.uuid4())
    
    peak_memory = process.memory_info().rss 

    # ---------- generate QR ----------
    qr = qrcode.QRCode(
        version=1,
        box_size=10,
        border=4
    )
    peak_memory = max(peak_memory, process.memory_info().rss)  # update peak memory after creating QR object
    qr.add_data(qr_data)
    qr.make(fit=True)

    img = qr.make_image(fill_color="black", back_color="white")
    peak_memory = max(peak_memory, process.memory_info().rss)  # update peak memory after processing
    # ---------- convert to bytes ----------
    buffer = io.BytesIO()
    peak_memory = max(peak_memory, process.memory_info().rss)  # update peak memory before saving
    img.save(buffer, format="PNG")
    img_bytes = buffer.getvalue()
    peak_memory = max(peak_memory, process.memory_info().rss)  # update peak memory after saving to buffer
    # ---------- store in redis ----------
    r.set(f"job:{job_id}:qr_image", img_bytes)

    # optional: base64 version for debugging
    r.set(
        f"job:{job_id}:qr_base64",
        base64.b64encode(img_bytes)
    )

    end = time.time()

    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user) +
        (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, arrival, start, end, cpu_total, peak_memory)

    return {
        "job_id": job_id,
        "status": "qr_generated"
    }
