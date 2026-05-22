#!/bin/bash
# Build and push Docker images (Face Detection) for each component, then deploy to Knative
set -e  # exit on error

BASE_IMAGE="172.0.0.0:5000"

services=(
  fd-f1-preprocess
  fd-f2-backbone
  fd-f3-classifier
  fd-f4-bbox
  fd-f5-landmark
  fd-f6-merge
)

for service in "${services[@]}"; do
  docker build -t "$BASE_IMAGE/$service:latest" "./$service"
  
  echo "$service built"
  docker push "$BASE_IMAGE/$service:latest"

  echo "$service Pushed"
  echo "------------------------"
done

