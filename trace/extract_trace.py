import os
import json
import csv
import re

# ---------------- CONFIG ----------------
INPUT_DIR = "redis_dump"
OUTPUT_CSV = "metrics.csv"

FUNCTION_PATTERN = re.compile(r"-f([1-8])-")

# ---------------- HELPERS ----------------
def safe_float(val):
    try:
        return float(val)
    except:
        return 0.0


def safe_str(val):
    return val if val is not None else ""


# ---------------- MAIN ----------------
def main():

    if not os.path.exists(INPUT_DIR):
        print(f"Folder '{INPUT_DIR}' not found")
        return

    files = [f for f in os.listdir(INPUT_DIR) if f.endswith(".json")]

    if not files:
        print(" No JSON files found")
        return

    print(f" Found {len(files)} files\n")

    jobs = {}

    print("==== PROCESSING FILES ====\n")

    # -------- STEP 1: GROUP BY JOB -------- #
    for file_name in files:

        file_path = os.path.join(INPUT_DIR, file_name)

        try:
            function_name = file_name.split("_", 2)[-1].replace(".json", "")
        except:
            continue

        match = FUNCTION_PATTERN.search(function_name)
        if not match:
            continue

        f_order = int(match.group(1))

        try:
            with open(file_path, "r") as f:
                data = json.load(f)
        except:
            continue

        job_uuid = data.get("job_id")

        row = {
            "function": safe_str(data.get("function")),
            "arrival_time": safe_float(data.get("arrival_time")),
            "start_time": safe_float(data.get("start_time")),
            "end_time": safe_float(data.get("end_time")),
            "processing_time_ms": safe_float(data.get("processing_time_ms")),
            "cpu_time_sec": safe_float(data.get("cpu_time_sec")),
            "memory_mb": safe_float(data.get("memory_mb")),
            "f_order": f_order
        }

        if job_uuid not in jobs:
            jobs[job_uuid] = []

        jobs[job_uuid].append(row)

    # -------- STEP 2: SORT FUNCTIONS INSIDE EACH JOB -------- #
    for job_uuid in jobs:
        jobs[job_uuid].sort(key=lambda x: x["f_order"])

    # -------- STEP 3: SORT JOBS BY FIRST ARRIVAL -------- #
    job_order = sorted(
        jobs.items(),
        key=lambda item: min(r["arrival_time"] for r in item[1])
    )

    # -------- STEP 4: ASSIGN JOB ID + FLATTEN -------- #
    final_rows = []
    job_id_counter = 1

    for job_uuid, job_rows in job_order:

        for row in job_rows:
            row["job_id"] = job_id_counter
            final_rows.append(row)

        job_id_counter += 1

    # NO MORE SORTING HERE 

    # -------- STEP 5: WRITE CSV -------- #
    with open(OUTPUT_CSV, "w", newline="") as f:

        writer = csv.DictWriter(
            f,
            fieldnames=[
                "job_id",
                "function",
                "arrival_time",
                "start_time",
                "end_time",
                "processing_time_ms",
                "cpu_time_sec",
                "memory_mb"
            ]
        )

        writer.writeheader()

        for row in final_rows:
            row.pop("f_order", None)
            writer.writerow(row)

    print(f"\nDone! Extracted {len(final_rows)} rows")
    print(f" Saved to {OUTPUT_CSV}\n")


# ---------------- RUN ----------------
if __name__ == "__main__":
    main()