from fastapi import FastAPI, Request
import redis, json, requests, psutil, time
import os

app = FastAPI()

r = redis.Redis(host="redis.default.svc.cluster.local", port=6379, decode_responses=True)

F4_URL = "http://sec-f4-risk.default.svc.cluster.local"

peak_memory = 0

def record_metrics(job_id, arrival, start, end, cpu_total, peak_memory):
    metrics = {
        "job_id": job_id,
        "function": "sec-f3-dast",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory/(1024 * 1024)
    }

    r.set(f"job:{job_id}:sec-f3-dast", json.dumps(metrics))
    print(json.dumps(metrics))


@app.post("/")
async def dast(request: Request):
    global peak_memory
    peak_memory = 0  # reset peak memory for this request
    arrival = time.time()
    data = await request.json()
    job_id = data["job_id"]

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()


    file_list = json.loads(r.get(f"job:{job_id}:file_list"))

    findings = []
    peak_memory = process.memory_info().rss  # memory after loading file list
    for obj in file_list:
        if obj.endswith(".html"):
            findings.append({
                "file": obj,
                "issue": "potential_xss"
            })
        peak_memory = max(peak_memory, process.memory_info().rss)  # update peak memory after processing each file

    r.set(f"job:{job_id}:dast_result", json.dumps(findings))

    end = time.time()
    cpu_end = process.cpu_times()
    cpu_total = (cpu_end.user - cpu_start.user) + (cpu_end.system - cpu_start.system)
    record_metrics(job_id, arrival, start, end, cpu_total, peak_memory)

    requests.post(F4_URL, json={"job_id": job_id, "source": "dast"})

    return {"status": "dast_done"}