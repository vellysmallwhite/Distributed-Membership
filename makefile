# Define the Docker image name
IMAGE_NAME = prj3

# Define the compose files for each test case
COMPOSE_FILE_1 = docker-compose-testcase-1.yml
COMPOSE_FILE_2 = docker-compose-testcase-2.yml
COMPOSE_FILE_3 = docker-compose-testcase-3.yml
COMPOSE_FILE_4 = docker-compose-testcase-4.yml

# Step 1: Remove any existing containers from previous runs
down:
	docker compose -f $(COMPOSE_FILE_1) down
	docker compose -f $(COMPOSE_FILE_2) down
	docker compose -f $(COMPOSE_FILE_3) down
	docker compose -f $(COMPOSE_FILE_4) down

# Step 2: Rebuild the Docker image
build:
	docker build . -t $(IMAGE_NAME)

# Step 3: Run Test Case 1
testcase1: down build
	docker compose -f $(COMPOSE_FILE_1) up 
# Step 4: Run Test Case 2
testcase2: down build
	docker compose -f $(COMPOSE_FILE_2) up 
# Step 5: Run Test Case 3
testcase3: down build
	docker compose -f $(COMPOSE_FILE_3) up 

# Step 6: Run Test Case 4
testcase4: down build
	docker compose -f $(COMPOSE_FILE_4) up 

# Step 7: Run all test cases sequentially
all: testcase1 testcase2 testcase3 testcase4
