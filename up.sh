#!/usr/bin/env bash
# Starts Kafka and creates one topic per event type listed in contracts/.
set -euo pipefail
cd "$(dirname "$0")"
docker compose up -d --wait
topics=$(uv run --directory contracts python -c \
  'from shop_contracts.events import EVENTS; print(" ".join(topic for topic, _ in EVENTS.values()))')
for topic in $topics; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$topic" --partitions 3 > /dev/null
done
echo "Kafka is up. Topics: $topics"
