#!/usr/bin/env bash
set -e  # exit on error

BASE_URL="172.0.0.0:5000"

services=(
  ml-f1-init
  ml-f2-split
  ml-f3-preprocess
  ml-f4-train
  ml-f5-evaluate
)

for service in "${services[@]}"; do
  docker build -t "$BASE_URL/$service:latest" "./$service"
  
  echo "$service built"
  docker push "$BASE_URL/$service:latest"

  echo "$service Pushed"
  echo "------------------------"
done
