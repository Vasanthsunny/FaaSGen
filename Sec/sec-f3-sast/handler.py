from fastapi import FastAPI, Request
import redis, json, os, re, requests, time, psutil
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

PATTERNS = {
    "hardcoded_password": r"(password\s*=\s*['\"].+['\"])",
    "eval_usage": r"\beval\("
}

def record_metrics(job_id, arrival, start, end, cpu_total, peak_memory):
    metrics = {
        "job_id": job_id,
        "function": "sec-f3-sast",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory/(1024 * 1024)
    }

    r.set(f"job:{job_id}:sec-f3-sast", json.dumps(metrics))
    print(json.dumps(metrics))



@app.post("/")
async def sast(request: Request):

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

        if not obj.endswith((".py", ".js", ".txt")):
            continue
        peak_memory = max(peak_memory, process.memory_info().rss)  # update peak memory after checking file type
        response = minio_client.get_object(BUCKET, obj)
        content = response.read().decode(errors="ignore")

        for name, pattern in PATTERNS.items():
            if re.search(pattern, content):
                findings.append({
                    "file": obj,
                    "issue": name
                })
        peak_memory = max(peak_memory, process.memory_info().rss)  # update peak memory after processing each file  
    r.set(f"job:{job_id}:sast_result", json.dumps(findings))

    end = time.time()   
    cpu_end = process.cpu_times()
    cpu_total = (cpu_end.user - cpu_start.user) + (cpu_end.system - cpu_start.system)

    record_metrics(job_id, arrival, start, end, cpu_total, peak_memory)


    # trigger barrier
    requests.post(F4_URL, json={"job_id": job_id, "source": "sast"})

    return {"status": "sast_done"}