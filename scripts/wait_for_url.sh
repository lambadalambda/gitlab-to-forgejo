#!/usr/bin/env bash
set -euo pipefail

url="${1:-http://localhost:3000/api/v1/version}"
timeout_s="${TIMEOUT_S:-120}"

start="$(date +%s)"
while true; do
  if curl -fsS --max-time 2 "$url" >/dev/null 2>&1; then
    exit 0
  fi

  now="$(date +%s)"
  if (( now - start > timeout_s )); then
    echo "Timed out waiting for $url after ${timeout_s}s" >&2
    exit 1
  fi

  sleep 1
done
