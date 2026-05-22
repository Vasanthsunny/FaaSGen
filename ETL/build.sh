#!/usr/bin/env bash
set -e  # exit on error

BASE_IMAGE="172.0.0.0:5000"

services=(
  etl-f1-acquire
  etl-f2-clean
  etl-f3-transform
  etl-f4-aggregate
  etl-f5-output
)

for service in "${services[@]}"; do
  docker build -t "$BASE_IMAGE/$service:latest" "./$service"
  
  echo "$service built"
  docker push "$BASE_IMAGE/$service:latest"

  echo "$service Pushed"
  echo "------------------------"
done

