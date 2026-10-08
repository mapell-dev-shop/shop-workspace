#!/usr/bin/env bash
# Usage: ./checkout-lesson.sh NN [--discard]
# Puts every shop repo on a branch lesson-NN that starts at the tag lesson-NN-start.
# Refuses, changing nothing, when a tag is missing or a repo has uncommitted changes.
# --discard throws uncommitted changes away instead.
set -euo pipefail
cd "$(dirname "$0")"

if [[ $# -lt 1 || ! "$1" =~ ^[0-9][0-9]$ || ( $# -eq 2 && "${2:-}" != "--discard" ) || $# -gt 2 ]]; then
  echo "usage: $0 NN [--discard]" >&2
  exit 2
fi
lesson=$1
discard=${2:-}
tag="lesson-$lesson-start"
repos=(. contracts orders-service inventory-service payments-service)

for repo in "${repos[@]}"; do
  if ! git -C "$repo" rev-parse -q --verify "refs/tags/$tag" > /dev/null; then
    echo "$repo: tag $tag not found" >&2
    exit 1
  fi
  if [[ -n "$(git -C "$repo" status --porcelain)" && "$discard" != "--discard" ]]; then
    echo "$repo has uncommitted changes. Commit them, or rerun with --discard." >&2
    exit 1
  fi
done

for repo in "${repos[@]}"; do
  git -C "$repo" checkout -q -f -B "lesson-$lesson" "$tag"
  if [[ "$discard" == "--discard" ]]; then
    git -C "$repo" clean -q -fd
  fi
done
echo "Every repo is on branch lesson-$lesson, starting at $tag."
