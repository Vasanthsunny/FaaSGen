from fastapi import FastAPI, Request
import numpy as np
import torch
import torchvision.models as models
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

# ---- ORIGINAL MODEL (auto download allowed) ----
weights = models.MobileNet_V2_Weights.DEFAULT
model = models.mobilenet_v2(weights=weights).features
model.eval()

# ---- Redis ----
r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=True
)

# ---- URLs ----
F3_URL = "http://fd-f3-classifier.default.svc.cluster.local"
F4_URL = "http://fd-f4-bbox.default.svc.cluster.local"
F5_URL = "http://fd-f5-landmark.default.svc.cluster.local"



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
async def backbone(request: Request):
    global peak_memory
    peak_memory = 0  # reset peak memory for this request
    arrival_time = time.time()

    data = await request.json()
    job_id = data["job_id"]
    tensor_b64 = data["tensor"]

    process = psutil.Process()

    # ---- CPU START ----
    cpu_start = process.cpu_times()
    start_time = time.time()
    peak_memory = process.memory_info().rss
    # ---- Decode tensor ----
    decoded = base64.b64decode(tensor_b64)
    buffer = io.BytesIO(decoded)
    np_tensor = np.load(buffer)
    peak_memory = max(peak_memory, process.memory_info().rss)
    torch_tensor = torch.from_numpy(np_tensor).float()

    if torch_tensor.ndim == 3:
        torch_tensor = torch_tensor.unsqueeze(0)
    peak_memory = max(peak_memory, process.memory_info().rss)
    # ---- Forward pass ----
    with torch.no_grad():
        features = model(torch_tensor)

    # ---- Serialize output ----
    buf = io.BytesIO()
    np.save(buf, features.cpu().numpy())
    peak_memory = max(peak_memory, process.memory_info().rss)
    encoded = base64.b64encode(buf.getvalue()).decode()

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
        "fd-f2-backbone",
        arrival_time,
        start_time,
        end_time,
        cpu_total,
        peak_memory
    )

    payload = {
        "job_id": job_id,
        "features": encoded
    }

    # Fire-and-forget parallel
    try:
        requests.post(F3_URL, json=payload)
        requests.post(F4_URL, json=payload)
        requests.post(F5_URL, json=payload)
    except:
        pass

    return {"job_id": job_id, "status": "fd-f2-done"}
