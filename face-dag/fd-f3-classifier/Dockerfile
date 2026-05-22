# ---------- Builder ----------
FROM python:3.10-slim as builder

WORKDIR /install

RUN apt-get update && apt-get install -y build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --prefix=/install --no-cache-dir -r requirements.txt


# ---------- Final ----------
FROM python:3.10-slim

WORKDIR /app

# Copy installed dependencies
COPY --from=builder /install /usr/local

# Copy app code
COPY handler.py .

EXPOSE 8080

CMD ["uvicorn", "handler:app", "--host", "0.0.0.0", "--port", "8080"]