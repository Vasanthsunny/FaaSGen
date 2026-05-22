#!/usr/bin/env bash
set -e  # exit on error

BASE_URL="172.0.0.0:5000"

services=(
  img-f1-input
  img-f2-resize-blur
  img-f3-edge-detect
  img-f4-aggregate-output
)

for service in "${services[@]}"; do
  docker build -t "$BASE_URL/$service:latest" "./$service"
  
  echo "$service built"
  docker push "$BASE_URL/$service:latest"

  echo "$service Pushed"
  echo "------------------------"
done
