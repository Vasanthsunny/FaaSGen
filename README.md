
# Serverless Workflow Benchmarks for Kubernetes & Knative

Overview

This repository contains multiple serverless workflow applications designed and tested on a Kubernetes + Knative platform.

Each workflow represents a Directed Acyclic Graph (DAG) of interconnected serverless functions. The workflows are organized into separate directories, where:

- Each workflow directory represents one complete application workflow.
- Each function directory inside a workflow represents a serverless function/node in the DAG.
- Function execution follows dependency order such as:

```text
f1 → f2 → f3 → ...
```
Functions communicate using HTTP-based JSON payloads. Depending on the workflow, data transfer occurs either through:

- Direct JSON payload exchange
- JSON references pointing to objects stored in MinIO (similar to AWS S3)

The repository is intended for benchmarking, experimentation, orchestration research, and serverless scheduling evaluation in edge-cloud or distributed Kubernetes environments.

---

## Repository Structure

```text
.
├── kapply_all.sh
├── LICENSE
├── README.md
│
├── Ecom/
├── ETL/
├── face-dag/
├── Img/
├── IOT/
├── Log/
├── MC/
├── MLT/
├── OCR/
├── QR/
└── Sec/
```
Deatiled workflow descriptions are  available in `Workflow.md `
---

## Workflow Directory Structure

Each workflow directory contains:
```text
workflow/
├── build.sh
├── services.yaml
├── function-1/
├── function-2/
└── ...
```
build.sh

Used to build all function container images for the workflow.

Example:

```bash
bash build.sh
```

services.yaml

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
```text
{
  "image": "...",
  "metadata": "...",
  "request_id": "123"
}
```
2. MinIO Object Reference Payload

Example:
```text
{
  "bucket": "workflow-data",
  "object": "input/file1.json"
}
```
---

# Platform Requirements

Core Requirements

- Kubernetes Cluster
- Knative Serving
- Docker
- MinIO Object Storage
- Redis Database
- Linux Environment (Ubuntu 22 or equivalent - recommended)

---

## Recommended Software Versions

Component	& Recommended Version

Kubernetes	v1.26+
Knative Serving	v1.10+
Docker	v24+
MinIO	Latest
Redis	v7+



# Cluster Architecture Requirements

At least one Kubernetes server node is required.

Recommended setup:

Node Type	 & Purpose

Master Node	Kubernetes control plane + Knative
Worker Node(s)	Workflow function execution

The cluster can be extended with multiple worker nodes depending on workload requirements.

---

## Worker Node Requirements

Each worker node should:

- Be connected to the Kubernetes master node
- Have Docker/container runtime installed
- Access the container registry
- Support Knative scheduling
- Have sufficient CPU and memory resources

---

## Local Container Registry

Each node should maintain access to a local/private container registry for storing workflow images.

Example local registry usage:
```bash
docker tag workflow-image localhost:5000/workflow-image
docker push localhost:5000/workflow-image
```
Benefits:

- Faster deployment
- Reduced external dependency
- Efficient multi-node image distribution

---

## Recommended Infrastructure

Minimum Setup

- 1 Master Node
- 1 Worker Node
- 8 GB RAM
- 4 CPU Cores

---

## Recommended Experimental Setup

- 1 Master Node
- 2–8 Worker Nodes
- 16+ GB RAM per node
- SSD storage
- Gigabit network

---

## Monitoring 

All application functions automatically logs the metrics in redis (See `trace_generation.md`) for each request.

---

# Building Workflows

Example:
```bash
cd ETL
bash build.sh
```
This builds all function container images for the workflow.

---

## Deploying Workflows

Example:
```bash
kubectl apply -f ETL/services.yaml
```
---

## Invocation Example

Example HTTP request:
```bash
curl -X POST http://SERVICE_URL \
-H "Content-Type: application/json" \
-d '{"input":"sample"}'
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

# Use Cases

These workflows can be used for:

- Serverless scheduling research
- Edge-cloud orchestration evaluation
- DAG scheduling benchmarks
- Knative performance testing
- Distributed workflow experimentation
- Resource allocation studies
- Function chaining evaluation
---

# Notes

- Ensure Docker images are properly tagged and pushed into local registry before deployment.
- Knative networking must be configured correctly for external HTTP access.
- MinIO credentials should be configured inside workflow functions.
- Redis is used for lightweight state sharing and caching and Metrics storage.
- Workflow execution order is DAG-based and may include parallel branches depending on the application.
---

# License

See the LICENSE file for license information.
