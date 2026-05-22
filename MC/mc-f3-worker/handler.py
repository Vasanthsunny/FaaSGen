from fastapi import FastAPI, Request
import numpy as np
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

# ---------- F4 Aggregator ----------
F4_URL = os.getenv(
    "F4_URL",
    "http://mc-f4-aggregate.default.svc.cluster.local/"
)

# ---------- metrics ----------
def record_metrics(job_id, scenario, batch_id, arrival, start, end, cpu_total,peak_memory):


    metrics = {
        "job_id": job_id,
        "scenario": scenario,
        "batch_id": batch_id,
        "function": "mc-f3-worker",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(
        f"job:{job_id}:mc-f3-worker:{scenario}:{batch_id}",
        json.dumps(metrics)
    )

    print(json.dumps(metrics))


@app.post("/")
async def monte_carlo_worker(request: Request):

    global peak_memory
    peak_memory = 0

    arrival = time.time()

    data = await request.json()

    required = [
        "job_id", "scenario", "batch_id",
        "S0", "K", "r", "sigma", "T", "paths"
    ]

    for k in required:
        if k not in data:
            return {"error": f"missing {k}"}

    job_id = data["job_id"]
    scenario = data["scenario"]
    batch_id = int(data["batch_id"])

    S0 = float(data["S0"])
    K = float(data["K"])
    r_val = float(data["r"])
    sigma = float(data["sigma"])
    T = float(data["T"])
    paths = int(data["paths"])

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()
    peak_memory = process.memory_info().rss
    # ---------- Monte Carlo simulation ----------
    Z = np.random.normal(0, 1, paths)

    ST = S0 * np.exp(
        (r_val - 0.5 * sigma**2) * T +
        sigma * np.sqrt(T) * Z
    )
    peak_memory = max(peak_memory, process.memory_info().rss)
    payoff = np.maximum(ST - K, 0)

    # ---------- statistics ----------
    payoff_sum = float(np.sum(payoff))
    payoff_sq_sum = float(np.sum(payoff ** 2))
    peak_memory = max(peak_memory, process.memory_info().rss)
    count = int(paths)

    result = {
        "sum": payoff_sum,
        "sum_sq": payoff_sq_sum,
        "count": count
    }
    
    r.set(
        f"job:{job_id}:result:{scenario}:{batch_id}",
        json.dumps(result)
    )

    # ---------- worker completion counter ----------
    done = r.incr(f"job:{job_id}:done:{scenario}")

    workers_val = r.get(f"job:{job_id}:workers:{scenario}")
    peak_memory = max(peak_memory, process.memory_info().rss)
    if workers_val is None:
        print("Workers key missing in Redis")
    else:

        workers = int(workers_val)

        if done >= workers:

            print("All workers finished → triggering F4")

            try:
                requests.post(
                    F4_URL,
                    json={
                        "job_id": job_id,
                        "scenario": scenario,
                        "r": r_val,
                        "T": T
                    },
                    timeout=5
                )

            except Exception as e:
                print("F4 trigger failed:", e)
    peak_memory = max(peak_memory, process.memory_info().rss)
    end = time.time()

    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user) +
        (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, scenario, batch_id, arrival, start, end, cpu_total, peak_memory)

    return {
        "job_id": job_id,
        "scenario": scenario,
        "batch_id": batch_id,
        "status": "completed"
    }
