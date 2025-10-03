#!/bin/bash
#  it takes one argument: "full", "restart", or "none" based on which files are changed in the commit (determined in GitHub Actions)
  #  "full" will pull the latest code, rebuild the containers, and restart them
  #  "restart" will restart the containers without pulling the latest code
  #  "none" will do nothing (used when only DAGs have changed)


ACTION=$1

# Only exit on error for the critical section
if [ "$ACTION" = "full" ]; then
  set -e
  docker-compose down
  docker-compose up --build -d
elif [ "$ACTION" = "restart" ]; then
  set -e
  docker-compose restart
elif [ "$ACTION" = "none" ]; then
  echo "Only DAGs changed; no container restart needed."
else
  echo "Unknown action: $ACTION"
  exit 1
fi

# Renew SSL certificates using Certbot
docker-compose run certbot certonly --webroot -w /var/www/certbot -d airflow.silverfund.byu.edu

if ! cmp -s /home/ec2-user/airflow/certbot-renew.service /etc/systemd/system/certbot-renew.service || \
   ! cmp -s /home/ec2-user/airflow/certbot-renew.timer /etc/systemd/system/certbot-renew.timer; then
  cp /home/ec2-user/airflow/certbot-renew.service /etc/systemd/system/certbot-renew.service
  cp /home/ec2-user/airflow/certbot-renew.timer /etc/systemd/system/certbot-renew.timer
  systemctl daemon-reload
  systemctl enable --now certbot-renew.timer
  echo "Systemd unit files updated and timer enabled."
else
  echo "No changes to systemd unit files; skipping reload."
fi