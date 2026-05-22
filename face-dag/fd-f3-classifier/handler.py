from fastapi import FastAPI, Request
import numpy as np
import torch
import base64
import io
import time
import psutil
import redis
import json
import requests
import uvicorn
peak_memory = 0
app = FastAPI()

# ---- Simple classifier head ----
classifier = torch.nn.Sequential(
    torch.nn.AdaptiveAvgPool2d((1,1)),
    torch.nn.Flatten(),
    torch.nn.Linear(1280, 2)
)
classifier.eval()

# ---- Redis ----
r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=True
)

# ---- F6 URL ----
F6_URL = "http://fd-f6-merge.default.svc.cluster.local"


def record_metrics(job_id, function_name, arrival, start, end, cpu_total, peak_memory):

    metrics = {
        "job_id": job_id,
        "function": function_name,
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(f"job:{job_id}:{function_name}", json.dumps(metrics))


@app.post("/")
async def classify(request: Request):
    global peak_memory
    peak_memory = 0  # reset peak memory for this request
    arrival_time = time.time()

    data = await request.json()
    job_id = data["job_id"]
    features_b64 = data["features"]

    process = psutil.Process()

    # ---- CPU START ----
    cpu_start = process.cpu_times()
    start_time = time.time()
    peak_memory = process.memory_info().rss
    # ---- Decode features ----
    decoded = base64.b64decode(features_b64)
    buffer = io.BytesIO(decoded)
    np_features = np.load(buffer)

    torch_features = torch.from_numpy(np_features).float()
    peak_memory = max(peak_memory, process.memory_info().rss)
    if torch_features.ndim == 3:
        torch_features = torch_features.unsqueeze(0)
    peak_memory = max(peak_memory, process.memory_info().rss)
    # ---- Forward pass ----
    with torch.no_grad():
        logits = classifier(torch_features)
    peak_memory = max(peak_memory, process.memory_info().rss)
    # ---- Serialize logits ----
    buf = io.BytesIO()
    np.save(buf, logits.numpy())
    encoded = base64.b64encode(buf.getvalue()).decode()
    peak_memory = max(peak_memory, process.memory_info().rss)
    # ---- CPU END ----
    end_time = time.time()
    cpu_end = process.cpu_times()

    cpu_user = cpu_end.user - cpu_start.user
    cpu_system = cpu_end.system - cpu_start.system
    cpu_total = cpu_user + cpu_system
    peak_memory = max(peak_memory, process.memory_info().rss)   

    # ---- Store metrics ----
    record_metrics(
        job_id,
        "fd-f3-classifier",
        arrival_time,
        start_time,
        end_time,
        cpu_total,
        peak_memory
    )

    # ---- Store output for F6 ----
    r.set(f"job:{job_id}:logits", encoded)

    # ---- Trigger F6 ----
    try:
        requests.post(F6_URL, json={"job_id": job_id})
    except:
        pass

    return {"job_id": job_id, "status": "fd-f3-done"}


