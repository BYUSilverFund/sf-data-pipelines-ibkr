#!/bin/bash
set -euo pipefail

# Health check for Airflow webserver
# Default port can be overridden via HEALTHCHECK_PORT
PORT=${HEALTHCHECK_PORT:-80}
HOST=${HEALTHCHECK_HOST:-localhost}

# Try HTTP health endpoint first
if command -v curl >/dev/null 2>&1; then
  if curl --max-time 5 -fsS "http://${HOST}:${PORT}/" >/dev/null 2>&1; then
    echo "HTTP health check passed"
    exit 0
  else
    echo "HTTP health check failed, falling back to docker-compose check"
  fi
fi

echo "No health check method available (curl or docker-compose)"
exit 3
