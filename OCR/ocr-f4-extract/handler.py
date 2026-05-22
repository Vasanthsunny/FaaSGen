from fastapi import FastAPI, Request
import redis
import json
import time
import psutil
import re
import os
import uvicorn

app = FastAPI()
peak_memory = 0
REDIS_HOST = os.getenv("REDIS_HOST", "redis.default.svc.cluster.local")

r = redis.Redis(
    host=REDIS_HOST,
    port=6379,
    decode_responses=True
)


# ---------- metrics ----------
def record_metrics(job_id, arrival, start, end, cpu_total,  peak_memory):

    metrics = {
        "job_id": job_id,
        "function": "ocr-f4-extract",
        "arrival_time": arrival,
        "start_time": start,
        "end_time": end,
        "processing_time_ms": (end - start) * 1000,
        "cpu_time_sec": cpu_total,
        "memory_mb": peak_memory / (1024 * 1024)
    }

    r.set(f"job:{job_id}:ocr-f4-extract", json.dumps(metrics))


# ---------- extraction logic ----------
def extract_invoice_fields(text):

    invoice_number = re.findall(
        r"(?:invoice\s*(?:no|number|#)?[:\s]*)\s*([A-Z0-9\-]+)",
        text,
        re.IGNORECASE
    )

    invoice_date = re.findall(
        r"(?:date[:\s]*)\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        text,
        re.IGNORECASE
    )

    total_amount = re.findall(
        r"(?:total\s*(?:amount)?[:\s]*)\s*\$?\s?(\d+(?:,\d{3})*(?:\.\d{2})?)",
        text,
        re.IGNORECASE
    )

    tax_amount = re.findall(
        r"(?:tax|vat)[:\s]*\$?\s?(\d+(?:,\d{3})*(?:\.\d{2})?)",
        text,
        re.IGNORECASE
    )

    emails = re.findall(
        r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
        text
    )

    phones = re.findall(
        r"\+?\d[\d\s\-]{8,15}",
        text
    )

    return {
        "invoice_number": invoice_number[0] if invoice_number else None,
        "invoice_date": invoice_date[0] if invoice_date else None,
        "total_amount": total_amount[-1] if total_amount else None,
        "tax_amount": tax_amount[0] if tax_amount else None,
        "emails": emails,
        "phones": phones
    }


@app.post("/")
async def extract(request: Request):
    global peak_memory
    peak_memory = 0  # reset peak memory for this request
    arrival = time.time()

    data = await request.json()
    job_id = data["job_id"]

    process = psutil.Process()
    cpu_start = process.cpu_times()
    start = time.time()

    # ---------- get merged OCR text ----------
    text = r.get(f"job:{job_id}:document_text")
    peak_memory = max(peak_memory, process.memory_info().rss)  # memory after fetching text
    if not text:
        return {"error": "document_text not found"}

    # ---------- store full text ---------- 
    r.set(
        f"job:{job_id}:document_txt",
        text
    )

    # ---------- extract invoice fields ----------
    invoice_data = extract_invoice_fields(text)
    peak_memory = max(peak_memory, process.memory_info().rss)  # memory after extraction
    r.set(
        f"job:{job_id}:invoice_fields",
        json.dumps(invoice_data)
    )

    end = time.time()

    cpu_end = process.cpu_times()

    cpu_total = (
        (cpu_end.user - cpu_start.user) +
        (cpu_end.system - cpu_start.system)
    )

    record_metrics(job_id, arrival, start, end, cpu_total, peak_memory)

    return {
        "job_id": job_id,
        "status": "invoice_extracted",
        "invoice_fields": invoice_data
    }
