FROM python:3.10-slim

WORKDIR /app


# install system dependencies
RUN apt-get update && \
    apt-get install -y poppler-utils && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY handler.py .


EXPOSE 8080

CMD ["uvicorn","handler:app","--host","0.0.0.0","--port","8080"]
