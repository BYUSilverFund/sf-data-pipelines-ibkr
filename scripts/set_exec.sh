#!/bin/bash
set -euo pipefail

# Make hook scripts in the extracted revision executable
if [ -d scripts ]; then
  chmod +x scripts/*.sh || true
fi

exit 0
