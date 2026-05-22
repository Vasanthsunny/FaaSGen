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
import torch.optim as optim
import requests
import os

peak_memory = 0
app = FastAPI()

r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=False
)

F5_URL = "http://ml-f5-evaluate.default.svc.cluster.local"

IMG_SIZE = 64
EPOCHS = 3


# ---------- CNN model ----------
class SimpleCNN(nn.Module):

    def __init__(self, num_classes):
        super(SimpleCNN, self).__init__()

        self.net = nn.Sequential(

            nn.Conv2d(3, 16, 3, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(2),

            nn.Conv2d(16, 32, 3, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(2),

            nn.Flatten(),

            nn.Linear(32 * (IMG_SIZE // 4) * (IMG_SIZE // 4), 128),
            nn.ReLU(),

            nn.Linear(128, num_classes)
        )

    def forward(self, x):
        return self.net(x)


# ---------- metrics ----------
def record_metrics(job_id, arrival, start, end, cpu_total, peak_memory):

      metrics = {
        "job_id": job_id,
        "function": "ml-f4-train",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(f"job:{job_id}:ml-f4-train", json.dumps(metrics))


@app.post("/")
async def train(request: Request):

    global peak_memory
    peak_memory = 0
    arrival = time.time()

    data = await request.json()
    job_id = data["job_id"]

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()
    peak_memory = process.memory_info().rss
    # ---------- load redis inputs ----------
    train_bytes = r.get(f"job:{job_id}:train_batches")
    class_map_bytes = r.get(f"job:{job_id}:class_map")
    peak_memory = max(peak_memory, process.memory_info().rss)
    if train_bytes is None:
        return {"error": "train_batches missing"}

    if class_map_bytes is None:
        return {"error": "class_map missing"}

    train_batches = pickle.loads(gzip.decompress(train_bytes))
    peak_memory = max(peak_memory, process.memory_info().rss)
    class_map = json.loads(class_map_bytes)
    peak_memory = max(peak_memory, process.memory_info().rss)
    num_classes = len(class_map)

    device = torch.device("cpu")

    model = SimpleCNN(num_classes).to(device)
    peak_memory = max(peak_memory, process.memory_info().rss)
    model.train()
    peak_memory = max(peak_memory, process.memory_info().rss)
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)
    peak_memory = max(peak_memory, process.memory_info().rss)
    # ---------- training ----------
    for epoch in range(EPOCHS):

        epoch_loss = 0

        for X, y in train_batches:

            X = torch.from_numpy(np.array(X)).permute(0,3,1,2).float().to(device)
            y = torch.tensor(y).long().to(device)

            optimizer.zero_grad()

            outputs = model(X)

            loss = criterion(outputs, y)

            peak_memory = max(peak_memory, process.memory_info().rss)
            loss.backward()

            optimizer.step()

            epoch_loss += loss.item()

        print(f"Epoch {epoch+1}/{EPOCHS} Loss: {epoch_loss:.4f}")

    # ---------- store trained model ----------
    model_bytes = pickle.dumps(model.state_dict())

    r.set(
        f"job:{job_id}:model",
        gzip.compress(model_bytes)
    )

    # ---------- cleanup ----------
    r.delete(f"job:{job_id}:train_batches")

    end = time.time()

    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user)
        + (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, arrival, start, end, cpu_total, peak_memory)

    # ---------- trigger evaluation ----------
    try:
        requests.post(F5_URL, json={"job_id": job_id}, timeout=5)
    except Exception as e:
        print("F5 trigger failed:", e)

    return {
        "job_id": job_id,
        "epochs": EPOCHS,
        "num_classes": num_classes,
        "status": "model_trained"
    }

