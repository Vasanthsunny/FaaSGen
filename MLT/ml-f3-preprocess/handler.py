from fastapi import FastAPI, Request
import redis
import json
import time
import psutil
import numpy as np
import gzip
import pickle
import uvicorn
import io
import tarfile
import requests
import os
from PIL import Image

peak_memory = 0
app = FastAPI()

r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=False
)

F4_URL = "http://ml-f4-train.default.svc.cluster.local"

IMG_SIZE = 64
BATCH_SIZE = 64


def record_metrics(job_id, arrival, start, end, cpu_total, peak_memory):

    metrics = {
        "job_id": job_id,
        "function": "ml-f3-preprocess",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(f"job:{job_id}:ml-f3-preprocess", json.dumps(metrics))


def preprocess_image(img):

    img = img.resize((IMG_SIZE, IMG_SIZE))
    img = np.array(img).astype(np.float32) / 255.0

    return img


def load_dataset_from_archive(archive_bytes, class_map):

    images = []
    labels = []

    tar_stream = io.BytesIO(archive_bytes)

    with tarfile.open(fileobj=tar_stream, mode="r:*") as tar:

        for m in tar.getmembers():

            if not m.isfile():
                continue

            parts = m.name.split("/")

            if len(parts) < 3:
                continue

            cls = parts[1]

            if cls not in class_map:
                continue

            f = tar.extractfile(m)

            if f is None:
                continue

            img = Image.open(io.BytesIO(f.read())).convert("RGB")

            images.append(img)
            labels.append(class_map[cls])

    return images, labels


@app.post("/")
async def preprocess(request: Request):

    global peak_memory
    peak_memory = 0
    arrival = time.time()

    data = await request.json()
    job_id = data["job_id"]

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()
    peak_memory = process.memory_info().rss
    # -------- read redis inputs --------
    train_archive = r.get(f"job:{job_id}:train")
    test_archive = r.get(f"job:{job_id}:test")
    peak_memory = max(peak_memory, process.memory_info().rss)
    class_map = json.loads(
        r.get(f"job:{job_id}:class_map")
    )

    # -------- load datasets --------
    train_images, train_labels = load_dataset_from_archive(train_archive, class_map)
    peak_memory = max(peak_memory, process.memory_info().rss)
    test_images, test_labels = load_dataset_from_archive(test_archive, class_map)
    peak_memory = max(peak_memory, process.memory_info().rss)
    train_batches = []
    test_batches = []

    # -------- build train batches --------
    for i in range(0, len(train_images), BATCH_SIZE):

        X = train_images[i:i + BATCH_SIZE]
        y = train_labels[i:i + BATCH_SIZE]

        X = [preprocess_image(img) for img in X]

        train_batches.append((np.array(X), np.array(y)))
    peak_memory = max(peak_memory, process.memory_info().rss)
    # -------- build test batches --------
    for i in range(0, len(test_images), BATCH_SIZE):

        X = test_images[i:i + BATCH_SIZE]
        y = test_labels[i:i + BATCH_SIZE]

        X = [preprocess_image(img) for img in X]

        test_batches.append((np.array(X), np.array(y)))
    peak_memory = max(peak_memory, process.memory_info().rss)
    # -------- store outputs --------
    r.set(
        f"job:{job_id}:train_batches",
        gzip.compress(pickle.dumps(train_batches))
    )

    r.set(
        f"job:{job_id}:test_batches",
        gzip.compress(pickle.dumps(test_batches))
    )

    # delete old data
    r.delete(f"job:{job_id}:train")
    r.delete(f"job:{job_id}:test")
    peak_memory = max(peak_memory, process.memory_info().rss)
    end = time.time()

    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user)
        + (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, arrival, start, end, cpu_total, peak_memory)

    if F4_URL:
        try:
            requests.post(F4_URL, json={"job_id": job_id}, timeout=5)
        except Exception as e:
            print("F4 trigger failed:", e)

    return {
        "job_id": job_id,
        "train_batches": len(train_batches),
        "test_batches": len(test_batches),
        "status": "preprocessed"
    }

