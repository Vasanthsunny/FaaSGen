from fastapi import FastAPI, Request
import uuid
import requests
import time
import psutil
import json
import os
import redis

peak_memory = 0
app = FastAPI()

# ---------- Redis connection ----------
REDIS_HOST = os.getenv(
    "REDIS_HOST",
    "redis.default.svc.cluster.local"
)

r = redis.Redis(
    host=REDIS_HOST,
    port=6379,
    decode_responses=True
)

# ---------- F2 service ----------
F2_URL = os.getenv(
    "F2_URL",
    "http://mc-f2-scenario.default.svc.cluster.local/"
)

# ---------- metrics ----------
def record_metrics(job_id, arrival, start, end, cpu_total,peak_memory):

    
    metrics = {
        "job_id": job_id,
        "function": "mc-f1-request",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(
        f"job:{job_id}:mc-f1-request",
        json.dumps(metrics)
    )

    print(json.dumps(metrics))


@app.post("/")
async def start_simulation(request: Request):

    global peak_memory
    peak_memory = 0  # reset peak memory for this request
    arrival = time.time()

    data = await request.json()

    required = [
        "S0", "K", "r", "sigma", "T",
        "scenarios", "paths_per_scenario", "batches"
    ]

    for key in required:
        if key not in data:
            return {"error": f"missing field {key}"}

    job_id = str(uuid.uuid4())

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()
    peak_memory = process.memory_info().rss
    S0 = float(data["S0"])
    K = float(data["K"])
    r_val = float(data["r"])
    sigma = float(data["sigma"])
    T = float(data["T"])
    peak_memory = max(peak_memory, process.memory_info().rss)
    scenarios = int(data["scenarios"])
    paths = int(data["paths_per_scenario"])
    batches = int(data["batches"])
    peak_memory = max(peak_memory, process.memory_info().rss)
    session = requests.Session()

    for i in range(scenarios):

        scenario_name = chr(65 + i)

        scenario_sigma = sigma * (1 + i * 0.2)

        payload = {
            "job_id": job_id,
            "scenario": scenario_name,
            "S0": S0,
            "K": K,
            "r": r_val,
            "sigma": scenario_sigma,
            "T": T,
            "paths_per_scenario": paths,
            "batches": batches
        }

        try:
            resp = session.post(F2_URL, json=payload, timeout=5)

            if resp.status_code != 200:
                print("F2 returned error:", resp.text)

        except Exception as e:
            print("F2 dispatch failed:", e)

    end = time.time()
    peak_memory = max(peak_memory, process.memory_info().rss)
    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user) +
        (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, arrival, start, end, cpu_total, peak_memory)

    return {
        "job_id": job_id,
        "status": "simulation_started"
    }
