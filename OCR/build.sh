#!/usr/bin/env bash
set -e  # exit on error

BASE_IMAGE="172.0.0.0:5000"

services=(
  ocr-f1-split
  ocr-f2-worker 
  ocr-f3-merge
  ocr-f4-extract
)

for service in "${services[@]}"; do
  docker build -t "$BASE_IMAGE/$service:latest" "./$service"
  
  echo "$service built"
  docker push "$BASE_IMAGE/$service:latest"

  echo "$service Pushed"
  echo "------------------------"
done
