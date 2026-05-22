from fastapi import FastAPI, Request
import redis
import json
import time
import psutil
import tarfile
import io
import random
import requests
import uvicorn

peak_memory = 0
app = FastAPI()

r = redis.Redis(
    host="redis.default.svc.cluster.local",
    port=6379,
    decode_responses=False
)

F3_URL = "http://ml-f3-preprocess.default.svc.cluster.local"


# -------- metrics ----------
def record(job_id, name, arrival, start, end, cpu_total,peak_memory):

   
    metrics = {
        "job_id": job_id,
        "function": name,
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(f"job:{job_id}:{name}", json.dumps(metrics))


@app.post("/")
async def split(request: Request):

    try:

        arrival = time.time()

        data = await request.json()
        job_id = data["job_id"]

        process = psutil.Process()
        cpu_start = process.cpu_times()
        start = time.time()
        peak_memory = process.memory_info().rss
        # ---------- read dataset ----------
        dataset_bytes = r.get(f"job:{job_id}:dataset")

        if dataset_bytes is None:
            return {"error": "dataset not found in redis"}

        tar_stream = io.BytesIO(dataset_bytes)

        class_map = {}
        all_files = []
        peak_memory = max(peak_memory, process.memory_info().rss)
        # ---------- read archive ----------
        with tarfile.open(fileobj=tar_stream, mode="r:*") as tar:

            for m in tar.getmembers():

                if not m.isfile():
                    continue

                parts = m.name.split("/")

                if len(parts) < 3:
                    continue

                cls = parts[1]

                if cls not in class_map:
                    class_map[cls] = len(class_map)

                f = tar.extractfile(m)
                if f is None:
                    continue

                img_bytes = f.read()

                all_files.append({
                    "name": m.name,
                    "data": img_bytes
                })

        if len(all_files) == 0:
            return {"error": "no images found"}

        # ---------- split ----------
        random.shuffle(all_files)
        peak_memory = max(peak_memory, process.memory_info().rss)
        split_idx = int(len(all_files) * 0.8)
        peak_memory = max(peak_memory, process.memory_info().rss)
        train_files = all_files[:split_idx]
        test_files = all_files[split_idx:]

        # ---------- create train archive ----------
        train_buffer = io.BytesIO()
        peak_memory = max(peak_memory, process.memory_info().rss)
        with tarfile.open(fileobj=train_buffer, mode="w:gz") as tar:

            for f in train_files:

                info = tarfile.TarInfo(name=f["name"])
                info.size = len(f["data"])

                tar.addfile(info, io.BytesIO(f["data"]))

        # ---------- create test archive ----------
        test_buffer = io.BytesIO()
        peak_memory = max(peak_memory, process.memory_info().rss)
        with tarfile.open(fileobj=test_buffer, mode="w:gz") as tar:

            for f in test_files:

                info = tarfile.TarInfo(name=f["name"])
                info.size = len(f["data"])

                tar.addfile(info, io.BytesIO(f["data"]))

        # ---------- store outputs ----------
        r.set(f"job:{job_id}:train", train_buffer.getvalue())
        r.set(f"job:{job_id}:test", test_buffer.getvalue())
        r.set(f"job:{job_id}:class_map", json.dumps(class_map))

        # delete original dataset
        r.delete(f"job:{job_id}:dataset")

        end = time.time()

        cpu_end = process.cpu_times()

        cpu_total = (
            (cpu_end.user - cpu_start.user) +
            (cpu_end.system - cpu_start.system)
        )

        record(job_id, "ml-f2-split", arrival, start, end, cpu_total, peak_memory)

        # trigger next function
        requests.post(F3_URL, json={"job_id": job_id})

        return {
            "job_id": job_id,
            "train_images": len(train_files),
            "test_images": len(test_files),
            "status": "split_complete"
        }

    except Exception as e:

        return {"error": str(e)}

