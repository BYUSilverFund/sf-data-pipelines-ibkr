#!/bin/bash
set -euo pipefail

DEP_ROOT=/opt/codedeploy-agent/deployment-root
CURRENT="${DEPLOYMENT_ID:-}"

if [ -z "$CURRENT" ]; then
  echo "DEPLOYMENT_ID not set; skipping cleanup"
  exit 0
fi

for d in "$DEP_ROOT"/*; do
  [ -d "$d" ] || continue
  if echo "$d" | grep -q "$CURRENT"; then
    echo "keeping current deployment $d"
  else
    echo "removing old deployment $d"
    rm -rf "$d" || true
  fi
done

exit 0
