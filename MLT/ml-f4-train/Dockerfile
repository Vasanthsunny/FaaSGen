FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir \
    torch==2.1.2+cpu \
    --index-url https://download.pytorch.org/whl/cpu
COPY handler.py .

EXPOSE 8080

CMD ["uvicorn", "handler:app", "--host", "0.0.0.0", "--port", "8080"]
