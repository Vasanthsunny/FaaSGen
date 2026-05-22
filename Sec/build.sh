#!/usr/bin/env bash
set -e  # exit on error

BASE_IMAGE="172.0.0.0:5000"

services=(
  sec-f1-upload
  sec-f2-unpack
  sec-f3-sast
  sec-f3-dep
  sec-f3-dast
  sec-f4-risk
  sec-f5-report
)

for service in "${services[@]}"; do
  docker build -t "$BASE_IMAGE/$service:latest" "./$service"
  
  echo "$service built"
  docker push "$BASE_IMAGE/$service:latest"

  echo "$service Pushed"
  echo "------------------------"
done
