from fastapi import FastAPI, Request
import redis
import json
import time
import psutil
import os
import requests

app = FastAPI()

peak_memory = 0

r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=True
)

F5_URL = "http://sec-f5-report.default.svc.cluster.local"


def record_metrics(job_id, arrival, start, end, cpu_total,peak_memory):

    metrics = {
        "job_id": job_id,
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "function": "sec-f4-risk",
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(f"job:{job_id}:sec-f4-risk", json.dumps(metrics))
    print(json.dumps(metrics))


@app.post("/")
async def barrier_and_score(request: Request):

    global peak_memory
    peak_memory = 0  # reset peak memory for this request
    arrival = time.time()

    data = await request.json()
    job_id = data["job_id"]

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()

    # ---------- check if already processed ----------
    if r.get(f"job:{job_id}:risk_done"):
        return {"status": "already_processed"}
    peak_memory = process.memory_info().rss  # memory after checking Redis
    # ---------- check all results ----------
    sast = r.get(f"job:{job_id}:sast_result")
    peak_memory = max(peak_memory, process.memory_info().rss)  # memory after getting SAST result
    dep = r.get(f"job:{job_id}:dep_result")
    dast = r.get(f"job:{job_id}:dast_result")
    peak_memory = max(peak_memory, process.memory_info().rss)  # memory after getting DAST result
    if not (sast and dep and dast):
        return {"status": "waiting"}

    # ---------- lock (avoid duplicate) ----------
    if not r.setnx(f"job:{job_id}:risk_lock", "1"):
        return {"status": "locked_by_other"}

    r.expire(f"job:{job_id}:risk_lock", 30)
    peak_memory = max(peak_memory, process.memory_info().rss)  # memory after acquiring lock
    # ---------- parse ----------
    sast = json.loads(sast)
    dep = json.loads(dep)
    dast = json.loads(dast)

    # ---------- scoring ----------
    sast_score = len(sast) * 5
    dep_score = len(dep) * 7
    dast_score = len(dast) * 6
    peak_memory = max(peak_memory, process.memory_info().rss)  # memory after scoring
    total_score = sast_score + dep_score + dast_score

    # normalize (0–100)
    risk_score = min(100, total_score)

    # ---------- severity ----------
    if risk_score > 70:
        severity = "HIGH"
    elif risk_score > 40:
        severity = "MEDIUM"
    else:
        severity = "LOW"

    result = {
        "job_id": job_id,
        "risk_score": risk_score,
        "severity": severity,
        "sast_issues": len(sast),
        "dep_issues": len(dep),
        "dast_issues": len(dast)
    }
    peak_memory = max(peak_memory, process.memory_info().rss)  # memory after preparing result
    # ---------- store ----------
    r.set(f"job:{job_id}:risk_score", json.dumps(result))
    r.set(f"job:{job_id}:risk_done", "1")

    # ---------- trigger F5 ----------
    try:
        requests.post(F5_URL, json={"job_id": job_id}, timeout=5)
    except Exception as e:
        print("F5 trigger failed:", e)

    end = time.time()

    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user) +
        (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, arrival, start, end, cpu_total, peak_memory)

    return {
        "job_id": job_id,
        "risk_score": risk_score,
        "severity": severity,
        "status": "completed"
    }