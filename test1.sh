#!/bin/bash

# Step 1: Stop and remove any existing containers from the previous run
docker compose -f docker-compose-testcase-1.yml down
# Step 1: Rebuild the Docker image
docker build . -t prj3

docker compose -f docker-compose-testcase-1.yml up
