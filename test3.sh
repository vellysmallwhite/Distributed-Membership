#!/bin/bash

docker compose -f docker-compose-testcase-3.yml down
# Step 1: Rebuild the Docker image
docker build . -t prj3

docker compose -f docker-compose-testcase-3.yml up