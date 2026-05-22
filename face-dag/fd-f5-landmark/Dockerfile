
FROM python:3.10-slim as builder
WORKDIR /install

RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --prefix=/install --no-cache-dir -r requirements.txt

FROM python:3.10-slim
WORKDIR /app

COPY --from=builder /install /usr/local


COPY handler.py .

EXPOSE 8080
CMD ["uvicorn", "handler:app", "--host", "0.0.0.0", "--port", "8080"]
