#!/usr/bin/env bash
# Clones the four service repos into this workspace and installs their dependencies.
# Usage: SHOP_GIT_BASE=<git URL prefix of the shop repos> ./setup.sh
set -euo pipefail
cd "$(dirname "$0")"
: "${SHOP_GIT_BASE:?set SHOP_GIT_BASE to the git URL prefix of the shop repos}"
for repo in contracts orders-service inventory-service payments-service; do
  [[ -d "$repo/.git" ]] || git clone -q "$SHOP_GIT_BASE/$repo.git" "$repo"
  uv sync -q --directory "$repo"
done
uv sync -q
echo "Ready. Next: ./up.sh && ./run-all.sh"
