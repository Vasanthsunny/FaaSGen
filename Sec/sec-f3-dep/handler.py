from fastapi import FastAPI, Request
import redis, json, requests
import os, psutil, time
from minio import Minio

app = FastAPI()
peak_memory = 0

r = redis.Redis(host="redis.default.svc.cluster.local", port=6379, decode_responses=True)

minio_client = Minio(
    "minio.default.svc.cluster.local:9000",
    access_key="admin",
    secret_key="password123",
    secure=False
)

BUCKET = "sec-artifacts"
F4_URL = "http://sec-f4-risk.default.svc.cluster.local"

VULN_LIBS = ["django", "log4j", "lodash"]

def record_metrics(job_id, arrival, start, end, cpu_total, peak_memory):
    metrics = {
        "job_id": job_id,
        "function": "sec-f3-dep",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory/(1024 * 1024)
    }

    r.set(f"job:{job_id}:sec-f3-dep", json.dumps(metrics))
    print(json.dumps(metrics))


@app.post("/")
async def dep(request: Request):
    global peak_memory
    peak_memory = 0  # reset peak memory for this request
    arrival = time.time()

    data = await request.json()
    job_id = data["job_id"]

    # Start CPU time measurement
    start = time.time()
    process = psutil.Process()
    cpu_start = process.cpu_times()

    peak_memory = process.memory_info().rss  # memory at start of processing

    file_list = json.loads(r.get(f"job:{job_id}:file_list"))

    findings = []

    for obj in file_list:

        if obj.endswith(("requirements.txt", "package.json")):

            content = minio_client.get_object(BUCKET, obj).read().decode()
            peak_memory = max(peak_memory, process.memory_info().rss)  # update peak memory after reading file
            for lib in VULN_LIBS:
                if lib in content:
                    findings.append({"lib": lib, "severity": "HIGH"})
        peak_memory = max(peak_memory, process.memory_info().rss)  # update peak memory after processing each file
    r.set(f"job:{job_id}:dep_result", json.dumps(findings))

    end = time.time()
    cpu_end = process.cpu_times()
    cpu_total = (cpu_end.user - cpu_start.user) + (cpu_end.system - cpu_start.system)
    
    record_metrics(job_id, arrival, start, end, cpu_total, peak_memory)

    requests.post(F4_URL, json={"job_id": job_id, "source": "dep"})

    return {"status": "dep_done"}