#!/usr/bin/env bash
set -e  # exit on error

BASE_IMAGE="172.0.0.0:5000"

services=(
  ecom-f1-receive
  ecom-f2-validate
  ecom-f3-inventory
  ecom-f4-payment
  ecom-f5-combine
  ecom-f6-notify
)

for service in "${services[@]}"; do
  docker build -t "$BASE_IMAGE/$service:latest" "./$service"
  
  echo "$service built"
  docker push "$BASE_IMAGE/$service:latest"

  echo "$service Pushed"
  echo "------------------------"
done

