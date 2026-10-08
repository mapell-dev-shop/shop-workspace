#!/usr/bin/env bash
# Stops what ./run-all.sh started and waits until it has exited, so the workers leave their
# Kafka consumer groups before anything else stops.
set -euo pipefail
cd "$(dirname "$0")"

# A pid file is ours only while its process is one of the shop's APIs or workers; after a
# reboot the same pid can belong to anything. Match the arguments, not the interpreter path:
# a framework Python on macOS shows up in ps under its Python.app path.
is_ours() { ps -p "$1" -o args= 2> /dev/null | grep -qE -- '-m (uvicorn [a-z]+\.api:app|[a-z]+\.worker)'; }

pids=()
for pidfile in .run/*.pid; do
  [[ -e "$pidfile" ]] || continue
  pid=$(cat "$pidfile")
  if is_ours "$pid"; then
    kill "$pid" 2> /dev/null || true
    pids+=("$pid")
  fi
  rm "$pidfile"
done

for _ in $(seq 20); do
  running=()
  for pid in ${pids[@]+"${pids[@]}"}; do
    if is_ours "$pid"; then running+=("$pid"); fi
  done
  [[ ${#running[@]} -eq 0 ]] && { echo "Stopped."; exit 0; }
  sleep 0.5
done
echo "Still running after 10 s: ${running[*]}" >&2
exit 1
