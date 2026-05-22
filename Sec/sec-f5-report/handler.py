from fastapi import FastAPI, Request
import redis
import json
import time
import psutil
import os

app = FastAPI()
peak_memory = 0
r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=True
)


def record_metrics(job_id, arrival, start, end, cpu_total,peak_memory):

    metrics = {
        "job_id": job_id,
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "function": "sec-f5-report",
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(f"job:{job_id}:sec-f5-report", json.dumps(metrics))
    print(json.dumps(metrics))


@app.post("/")
async def generate_report(request: Request):

    global peak_memory
    peak_memory = 0  # reset peak memory for this request

    arrival = time.time()

    data = await request.json()
    job_id = data["job_id"]

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()

    # ---------- fetch results ----------
    sast = r.get(f"job:{job_id}:sast_result")
    dep = r.get(f"job:{job_id}:dep_result")
    peak_memory = process.memory_info().rss 
    dast = r.get(f"job:{job_id}:dast_result")
    risk = r.get(f"job:{job_id}:risk_score")
    peak_memory = max(peak_memory, process.memory_info().rss)  # memory after fetching all results
    if not (sast and dep and dast and risk):
        return {"error": "missing data"}

    sast = json.loads(sast)
    dep = json.loads(dep)
    peak_memory = max(peak_memory, process.memory_info().rss)  # memory after parsing sast and dep
    dast = json.loads(dast)
    risk = json.loads(risk)

    # ---------- build report ----------
    report = {
        "job_id": job_id,
        "summary": {
            "risk_score": risk["risk_score"],
            "severity": risk["severity"]
        },
        "counts": {
            "sast": len(sast),
            "dependency": len(dep),
            "dast": len(dast)
        },
        "details": {
            "sast": sast,
            "dependency": dep,
            "dast": dast
        }
    }

    # ---------- store ----------
    r.set(f"job:{job_id}:final_report", json.dumps(report))
    peak_memory = max(peak_memory, process.memory_info().rss)  # memory after storing report
    # ---------- print ----------
    print("\n========== SECURITY REPORT ==========")
    print(f"Job ID: {job_id}")
    print(f"Risk Score: {risk['risk_score']}")
    print(f"Severity: {risk['severity']}")
    print("------------------------------------")
    print(f"SAST Issues: {len(sast)}")
    print(f"Dependency Issues: {len(dep)}")
    print(f"DAST Issues: {len(dast)}")
    print("====================================\n")

    end = time.time()

    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user) +
        (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, arrival, start, end, cpu_total,peak_memory)

    return {
        "job_id": job_id,
        "status": "report_generated",
        "risk_score": risk["risk_score"]
    }