from fastapi import FastAPI, Request
import redis
import json
import time
import psutil
import os
import numpy as np

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

# ---------- metrics ----------
def record_metrics(job_id, scenario, arrival, start, end, cpu_total, peak_memory):

    metrics = {
        "job_id": job_id,
        "scenario": scenario,
        "function": "mc-f4-aggregate",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(
        f"job:{job_id}:mc-f4-aggregate:{scenario}",
        json.dumps(metrics)
    )

    print(json.dumps(metrics))


@app.post("/")
async def aggregate(request: Request):

    global peak_memory
    peak_memory = 0
    arrival = time.time()

    data = await request.json()
    print("F4 received payload:", data)

    job_id = data.get("job_id")
    scenario = data.get("scenario")

    if job_id is None or scenario is None:
        return {"status": "invalid_request"}

    r_val = float(data.get("r", 0))
    T = float(data.get("T", 1))

    process = psutil.Process()

    cpu_start = process.cpu_times()
    start = time.time()
    peak_memory = process.memory_info().rss
    # ---------- number of workers ----------
    workers_key = f"job:{job_id}:workers:{scenario}"
    workers_val = r.get(workers_key)

    if workers_val is None:
        return {"status": "waiting_workers"}

    try:
        workers = int(workers_val)
    except ValueError:
        return {"status": "invalid_workers"}
    peak_memory = max(peak_memory, process.memory_info().rss)
    total_sum = 0.0
    total_sq = 0.0
    total_count = 0

    # ---------- collect worker results ----------
    for worker_id in range(workers):

        key = f"job:{job_id}:result:{scenario}:{worker_id}"
        result = r.get(key)

        if result is None:
            return {"status": "waiting", "missing_worker": worker_id}

        result = json.loads(result)

        total_sum += float(result.get("sum", 0))
        total_sq += float(result.get("sum_sq", 0))
        total_count += int(result.get("count", 0))
    peak_memory = max(peak_memory, process.memory_info().rss)
    if total_count == 0:
        return {"status": "no_results"}

    # ---------- compute statistics ----------
    mean_payoff = total_sum / total_count
    variance = (total_sq / total_count) - (mean_payoff ** 2)
    variance = max(0.0, variance)
    peak_memory = max(peak_memory, process.memory_info().rss)
    option_price = np.exp(-r_val * T) * mean_payoff

    stats = {
        "scenario": scenario,
        "paths": total_count,
        "option_price": option_price,
        "variance": variance
    }

    r.set(
        f"job:{job_id}:scenario:{scenario}:stats",
        json.dumps(stats)
    )

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
        "status": "aggregated",
        "option_price": option_price
    }
