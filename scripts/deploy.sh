#!/bin/bash
# 1. Navigate to your Airflow directory on the EC2
TARGET_DIR="/home/ec2-user/airflow"
cd $TARGET_DIR

# 2. Pull the latest code from GitHub
git pull

# 3. Restart the containers with the new code
# --build ensures that if you changed a Dockerfile, it builds it locally
# -d runs it in the background
docker-compose down
docker-compose up --build -d

# 4. Optional: Clean up old Docker images to save disk space
docker image prune -f