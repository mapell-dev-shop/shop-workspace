#!/usr/bin/env bash
# Clones the four service repos into this workspace and installs their dependencies.
# They come from github.com/mapell-dev-shop; set SHOP_GIT_BASE to clone your forks instead.
set -euo pipefail
cd "$(dirname "$0")"
SHOP_GIT_BASE=${SHOP_GIT_BASE:-https://github.com/mapell-dev-shop}
for repo in contracts orders-service inventory-service payments-service; do
  [[ -d "$repo/.git" ]] || git clone -q "$SHOP_GIT_BASE/$repo.git" "$repo"
  uv sync -q --directory "$repo"
done
uv sync -q
echo "Ready. Next: ./up.sh && ./run-all.sh"
