#!/usr/bin/env bash
# Stops what ./run-all.sh started.
set -euo pipefail
cd "$(dirname "$0")"
for pidfile in .run/*.pid; do
  [[ -e "$pidfile" ]] || continue
  kill "$(cat "$pidfile")" 2> /dev/null || true
  rm "$pidfile"
done
echo "Stopped."
