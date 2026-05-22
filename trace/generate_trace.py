import asyncio
import aiohttp
import yaml
import random
import time
import os
import json
import base64
import csv

# ---------------- LOAD CONFIG ---------------- #
with open("config.yaml") as f:
    config = yaml.safe_load(f)

GLOBAL = config["global"]
WORKFLOWS = config["workflows"]
BURST = config["burst"]
COLD = config.get("cold_start", {"enabled": False})

# ---------------- GLOBALS ---------------- #
semaphore = asyncio.Semaphore(GLOBAL["max_in_flight"])
csv_lock = asyncio.Lock()

trace_file = open("trace.csv", "w", newline="")
csv_writer = csv.writer(trace_file)

csv_writer.writerow([
    "request_id", "workflow", "start_time", "end_time",
    "response_time", "job_id", "status", "cold_start"
])

# Track last invocation per workflow
last_invocation_time = {}

# ---------------- PAYLOAD HANDLER ---------------- #
def load_payload(workflow):
    payload_dir = workflow["payload_dir"]

    if not os.path.exists(payload_dir):
        raise Exception(f"Missing dir: {payload_dir}")

    files = os.listdir(payload_dir)
    if not files:
        raise Exception(f"No files in: {payload_dir}")

    file = random.choice(files)
    path = os.path.join(payload_dir, file)

    mode = workflow["payload_mode"]

    if mode == "binary":
        with open(path, "rb") as f:
            return f.read()

    elif mode == "json_file":
        with open(path, "r") as f:
            return json.load(f)

    elif mode == "base64":
        with open(path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode()
        return {"artifact_base64": encoded}

    else:
        raise ValueError(f"Unknown payload mode: {mode}")

# ---------------- COLD START DETECTION ---------------- #
def detect_cold_start(workflow_name, current_time):
    if not COLD["enabled"]:
        return False

    last_time = last_invocation_time.get(workflow_name)

    if last_time is None:
        last_invocation_time[workflow_name] = current_time
        return True  # first request = cold

    idle_time = current_time - last_time
    last_invocation_time[workflow_name] = current_time

    return idle_time > COLD["idle_threshold"]

# ---------------- REQUEST EXECUTION ---------------- #
async def send_request(session, request_id, workflow):
    async with semaphore:
        url = GLOBAL["gateway_url"]

        headers = {
            "Host": workflow["host"],
            "Content-Type": workflow["content_type"]
        }

        payload = load_payload(workflow)

        start_time = time.time()
        end_time = start_time
        job_id = None
        status = "failed"

        cold_start_flag = detect_cold_start(workflow["name"], start_time)

        timeout = aiohttp.ClientTimeout(total=GLOBAL["timeout"])

        for attempt in range(2):
            try:
                async with session.post(
                    url,
                    headers=headers,
                    json=payload if isinstance(payload, dict) else None,
                    data=payload if not isinstance(payload, dict) else None,
                    timeout=timeout
                ) as resp:

                    text = await resp.text()
                    end_time = time.time()

                    try:
                        response_json = json.loads(text)
                        job_id = response_json.get("job_id", None)
                    except Exception:
                        pass

                    status = resp.status
                    break

            except Exception as e:
                print(f"[ERROR] Request {request_id}: {e}")
                if attempt == 1:
                    end_time = time.time()
                await asyncio.sleep(0.1)

        response_time = end_time - start_time

        async with csv_lock:
            csv_writer.writerow([
                request_id,
                workflow["name"],
                start_time,
                end_time,
                response_time,
                job_id,
                status,
                cold_start_flag
            ])
            trace_file.flush()

# ---------------- POISSON ARRIVAL ---------------- #
def poisson_interval(rate):
    return random.expovariate(rate)

# ---------------- BURST LOGIC ---------------- #
def is_burst_time(start_time):
    if not BURST["enabled"]:
        return False

    elapsed = time.time() - start_time
    return int(elapsed) % BURST["interval"] < BURST["duration"]

# ---------------- MAIN GENERATOR ---------------- #
async def generate_requests():
    tasks = []

    async with aiohttp.ClientSession() as session:
        request_id = 0
        start_time = time.time()

        # ---- WARMUP (NOT COUNTED) ---- #
        for _ in range(GLOBAL["warmup_requests"]):
            wf = random.choice(WORKFLOWS)
            asyncio.create_task(send_request(session, f"warmup-{_}", wf))
            await asyncio.sleep(0.05)

        # ---- MAIN LOAD ---- #
        while request_id < GLOBAL["total_requests"]:
            wf = random.choice(WORKFLOWS)

            task = asyncio.create_task(
                send_request(session, request_id, wf)
            )
            tasks.append(task)

            request_id += 1

            rate = GLOBAL["arrival_rate"]

            if is_burst_time(start_time):
                rate *= BURST["multiplier"]

            interval = poisson_interval(rate)
            await asyncio.sleep(interval)

        # Wait for all requests to complete
        await asyncio.gather(*tasks)

# ---------------- RUN ---------------- #
async def main():
    await generate_requests()
    trace_file.close()

if __name__ == "__main__":
    asyncio.run(main())