from fastapi import FastAPI, Request
import requests
import time
import psutil
import json
import os
import redis

peak_memory = 0
app = FastAPI()

# ---------- Redis ----------
REDIS_HOST = os.getenv(
    "REDIS_HOST",
    "redis.default.svc.cluster.local"
)

r = redis.Redis(
    host=REDIS_HOST,
    port=6379,
    decode_responses=True
)

# ---------- F3 Worker ----------
F3_URL = os.getenv(
    "F3_URL",
    "http://mc-f3-worker.default.svc.cluster.local/"
)

# ---------- Worker Limit ----------
MAX_WORKERS = 6


# ---------- metrics ----------
def record_metrics(job_id, scenario, arrival, start, end, cpu_total, peak_memory):

    p = psutil.Process()

    metrics = {
        "job_id": job_id,
        "scenario": scenario,
        "function": "mc-f2-scenario",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(
        f"job:{job_id}:mc-f2-scenario:{scenario}",
        json.dumps(metrics)
    )

    print(json.dumps(metrics))


@app.post("/")
async def scenario_controller(request: Request):

    global peak_memory
    peak_memory = 0

    arrival = time.time()

    data = await request.json()

    required = [
        "job_id", "scenario", "S0", "K", "r",
        "sigma", "T", "paths_per_scenario", "batches"
    ]

    for key in required:
        if key not in data:
            return {"error": f"missing field {key}"}

    job_id = data["job_id"]
    scenario = data["scenario"]

    S0 = data["S0"]
    K = data["K"]
    r_val = data["r"]
    sigma = data["sigma"]
    T = data["T"]

    paths = int(data["paths_per_scenario"])
    batches = int(data["batches"])

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()
    peak_memory = process.memory_info().rss
    # ---------- determine worker count ----------
    workers = min(batches, MAX_WORKERS)
    peak_memory = max(peak_memory, process.memory_info().rss)
    # paths per worker
    base_paths = paths // workers
    remainder = paths % workers

    # store worker count for F3/F4 aggregation
    r.set(f"job:{job_id}:workers:{scenario}", workers)

    session = requests.Session()
    peak_memory = max(peak_memory, process.memory_info().rss)
    # ---------- dispatch Monte Carlo workers ----------
    for worker_id in range(workers):

        worker_paths = base_paths

        # last worker gets remainder
        if worker_id == workers - 1:
            worker_paths += remainder

        payload = {
            "job_id": job_id,
            "scenario": scenario,
            "batch_id": worker_id,
            "S0": S0,
            "K": K,
            "r": r_val,
            "sigma": sigma,
            "T": T,
            "paths": worker_paths
        }

        try:
            session.post(F3_URL, json=payload, timeout=5)
        except Exception as e:
            print("F3 dispatch failed:", e)
    peak_memory = max(peak_memory, process.memory_info().rss)
    end = time.time()

    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user) +
        (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, scenario, arrival, start, end, cpu_total, peak_memory)

    return {
        "job_id": job_id,
        "scenario": scenario,
        "workers": workers,
        "status": "workers_dispatched"
    }
