from fastapi import FastAPI, Request
import redis
import json
import time
import psutil
import numpy as np
import gzip
import pickle
import uvicorn
import torch
import torch.nn as nn
import os

peak_memory = 0
app = FastAPI()

r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=False
)

IMG_SIZE = 64


# ---------- CNN model ----------
class SimpleCNN(nn.Module):

    def __init__(self, num_classes):
        super(SimpleCNN, self).__init__()

        self.net = nn.Sequential(

            nn.Conv2d(3,16,3,padding=1),
            nn.ReLU(),
            nn.MaxPool2d(2),

            nn.Conv2d(16,32,3,padding=1),
            nn.ReLU(),
            nn.MaxPool2d(2),

            nn.Flatten(),

            nn.Linear(32*(IMG_SIZE//4)*(IMG_SIZE//4),128),
            nn.ReLU(),

            nn.Linear(128,num_classes)
        )

    def forward(self,x):
        return self.net(x)


# ---------- metrics ----------
def record_metrics(job_id, arrival, start, end, cpu_total, peak_memory):

  
    metrics = {
        "job_id": job_id,
        "function": "ml-f5-evaluate",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end-start)*1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(f"job:{job_id}:ml-f5-evaluate", json.dumps(metrics))


@app.post("/")
async def evaluate(request: Request):

    global peak_memory
    peak_memory = 0
    arrival = time.time()

    data = await request.json()
    job_id = data["job_id"]

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()
    peak_memory = process.memory_info().rss

    # ---------- load metadata ----------
    class_map_bytes = r.get(f"job:{job_id}:class_map")

    if class_map_bytes is None:
        return {"error": "class_map missing"}
    peak_memory = max(peak_memory, process.memory_info().rss)
    class_map = json.loads(class_map_bytes)
    NUM_CLASSES = len(class_map)

    # ---------- load model ----------
    model_bytes = r.get(f"job:{job_id}:model")

    if model_bytes is None:
        return {"error": "model not found"}

    model_state = pickle.loads(gzip.decompress(model_bytes))
    peak_memory = max(peak_memory, process.memory_info().rss)
    model = SimpleCNN(NUM_CLASSES)
    model.load_state_dict(model_state)

    device = torch.device("cpu")

    model.to(device)
    model.eval()

    # ---------- load test batches ----------
    test_data = r.get(f"job:{job_id}:test_batches")
    peak_memory = max(peak_memory, process.memory_info().rss)
    if test_data is None:
        return {"error": "test_batches missing"}

    test_batches = pickle.loads(gzip.decompress(test_data))
    peak_memory = max(peak_memory, process.memory_info().rss)
    correct = 0
    total = 0

    confusion = np.zeros((NUM_CLASSES, NUM_CLASSES), dtype=int)

    # ---------- evaluation ----------
    with torch.no_grad():

        for X, y in test_batches:

            X = torch.from_numpy(np.array(X)).permute(0,3,1,2).float().to(device)
            y = torch.tensor(y).long().to(device)

            outputs = model(X)

            preds = torch.argmax(outputs, dim=1)
            peak_memory = max(peak_memory, process.memory_info().rss)
            correct += (preds == y).sum().item()
            total += y.size(0)

            for t,p in zip(y,preds):
                confusion[t.item()][p.item()] += 1

    accuracy = correct / total if total > 0 else 0

    results = {
        "accuracy": accuracy,
        "samples": total,
        "confusion_matrix": confusion.tolist()
    }

    r.set(
        f"job:{job_id}:evaluation",
        json.dumps(results)
    )

    # cleanup
    r.delete(f"job:{job_id}:test_batches")
    peak_memory = max(peak_memory, process.memory_info().rss)
    end = time.time()

    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user)
        +
        (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, arrival, start, end, cpu_total, peak_memory)

    return {
        "job_id": job_id,
        "accuracy": accuracy,
        "samples": total,
        "status": "evaluation_complete"
    }
