from fastapi import FastAPI, Request
import redis
import json
import time
import psutil
import os
import uuid
from PIL import Image
import io
import base64
import requests

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
    decode_responses=False
)


# ---------- metrics ----------
def record_metrics(job_id, arrival, start, end, cpu_total, peak_memory):
   
    metrics = {
        "job_id": job_id,
        "function": "tg-f1-gen",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(
        f"job:{job_id}:tg-f1-gen",
        json.dumps(metrics)
    )

    print(json.dumps(metrics))


@app.post("/")
async def generate_thumbnail(request: Request):

    global peak_memory
    peak_memory = 0  # reset peak memory for this request


    arrival = time.time()

    data = await request.json()

    job_id = str(uuid.uuid4())

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()

    # ---------- read size ----------
    size = int(data.get("size", 128))
    # initial memory before processing
    peak_memory = process.memory_info().rss  
    # ---------- load image ----------
    img_bytes = None

    if "image" in data:
        img_bytes = base64.b64decode(data["image"])

    elif "image_url" in data:
        try:
            resp = requests.get(data["image_url"], timeout=5)
            img_bytes = resp.content
        except Exception:
            return {"error": "failed to fetch image"}

    else:
        return {"error": "provide image or image_url"}

    # ---------- open image ----------
    try:
        img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
        peak_memory = max(peak_memory, process.memory_info().rss)  # memory after opening image
    except Exception:
        return {"error": "invalid image"}

    # ---------- generate thumbnail ----------
    img.thumbnail((size, size))
    peak_memory = max(peak_memory, process.memory_info().rss)  # memory after generating thumbnail
    buffer = io.BytesIO()
    img.save(buffer, format="JPEG")
    thumb_bytes = buffer.getvalue()

    # ---------- store in redis ----------
    r.set(f"job:{job_id}:thumbnail", thumb_bytes)
    peak_memory = max(peak_memory, process.memory_info().rss)  # memory after storing in Redis
    # optional base64 for debug
    r.set(
        f"job:{job_id}:thumbnail_base64",
        base64.b64encode(thumb_bytes)
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
        "status": "thumbnail_generated",
        "size": size
    }
