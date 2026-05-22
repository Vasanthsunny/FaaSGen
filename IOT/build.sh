#!/usr/bin/env bash
set -e  # exit on error

BASE_URL="172.0.0.0:5000"

services=(
  iot-f1-ingest
  iot-f2-decode
  iot-f3-filter
  iot-f4-aggregate
  iot-f5-dashboard
)

for service in "${services[@]}"; do
  docker build -t "$BASE_URL/$service:latest" "./$service"
  
  echo "$service built"
  docker push "$BASE_URL/$service:latest"

  echo "$service Pushed"
  echo "------------------------"
done
