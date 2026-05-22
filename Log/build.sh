#!/usr/bin/env bash
set -e  # exit on error

BASE_URL="172.0.0.0:5000"

services=(
  log-f1-batch
  log-f2-shard
  log-f3-parse
  log-f4-extract
  log-f5-sessionize
  log-f6-error-detect
  log-f7-global-aggregate
  log-f8-publish-metrics
)

for service in "${services[@]}"; do
  docker build -t "$BASE_URL/$service:latest" "./$service"
  
  echo "$service built"
  docker push "$BASE_URL/$service:latest"

  echo "$service Pushed"
  echo "------------------------"
done
