#!/usr/bin/env bash
set -e  # exit on error

BASE_URL="172.0.0.0:5000"

services=(
  mc-f1-request
  mc-f2-scenario
  mc-f3-worker
  mc-f4-aggregate
)

for service in "${services[@]}"; do
  docker build -t "$BASE_URL/$service:latest" "./$service"
  
  echo "$service built"
  docker push "$BASE_URL/$service:latest"

  echo "$service Pushed"
  echo "------------------------"
done
