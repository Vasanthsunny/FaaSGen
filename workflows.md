
# Workflow Descriptions



---

## Ecom — E-Commerce Workflow

- ecom-f1-receive
- ecom-f2-validate
- ecom-f3-inventory
- ecom-f4-payment
- ecom-f5-combine
* ecom-f6-notify

Simulates an e-commerce order processing pipeline.

---

## ETL — Extract Transform Load Workflow

etl-f1-acquire
etl-f2-clean
etl-f3-transform
etl-f4-aggregate
etl-f5-output

Implements a typical ETL data-processing pipeline.

---
## face-dag — Face Detection Workflow

fd-f1-preprocess
fd-f2-backbone
fd-f3-classifier
fd-f4-bbox
fd-f5-landmark
fd-f6-merge

Performs face detection and facial analysis operations.

---

## Img — Image Processing Workflow

img-f1-input
img-f2-resize-blur
img-f3-edge-detect
img-f4-aggregate-output

Processes image transformation and edge-detection operations.

---

## IOT — IoT Stream Processing Workflow

iot-f1-ingest
iot-f2-decode
iot-f3-filter
iot-f4-aggregate
iot-f5-dashboard

Simulates IoT telemetry ingestion and analytics pipelines.

---

## Log — Distributed Log Analytics Workflow

log-f1-batch
log-f2-shard
log-f3-parse
log-f4-extract
log-f5-sessionize
log-f6-error-detect
log-f7-global-aggregate
log-f8-publish-metrics

Implements large-scale distributed log-processing pipelines.

---

## MC — Monte Carlo Workflow

mc-f1-request
mc-f2-scenario
mc-f3-worker
mc-f4-aggregate

Performs distributed Monte Carlo simulations.

---

## MLT — Machine Learning Training Workflow

ml-f1-init
ml-f2-split
ml-f3-preprocess
ml-f4-train
ml-f5-evaluate

Implements an ML preprocessing and training pipeline.

---

## OCR — Optical Character Recognition Workflow

ocr-f1-split
ocr-f2-worker
ocr-f3-merge
ocr-f4-extract

Processes OCR extraction tasks using distributed workers.

---

## QR — QR Generation Workflow

qr-f1-gen

Simple QR code generation workflow.

---

## Sec — Security Scanning Workflow

sec-f1-upload
sec-f2-unpack
sec-f3-dep
sec-f3-sast
sec-f3-dast
sec-f4-risk
sec-f5-report

Implements security scanning and vulnerability assessment pipelines.

---

## TG — Thumbnail Generation Workflow

tg-f1-gen

Simple thumbnail-generation serverless workflow.

---

## Workflow Directory Structure

Each workflow directory contains:

workflow/
├── build.sh
├── services.yaml
├── function-1/
├── function-2/
└── ...

#### build.sh

Used to build all function container images for the workflow.

Example:

```bash
bash build.sh
```


## services.yaml

Contains Knative service definitions used for deployment.

Deploy workflow services:

```bash
kubectl apply -f services.yaml
```

---

## Global Deployment

Deploy All Workflows

The repository includes:
kapply_all.sh

This script deploys all workflow services automatically.

Run:

```bash
kapply_all.sh
```

---

## Communication Model

All functions are triggered through HTTP requests.

Payload Types

1. Direct JSON Payload

Example:

{
  "image": "...",
  "metadata": "...",
  "request_id": "123"
}

2. MinIO Object Reference Payload

Example:

{
  "bucket": "workflow-data",
  "object": "input/file1.json"
}
