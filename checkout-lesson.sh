#!/usr/bin/env bash
# Usage: ./checkout-lesson.sh NN [--discard]
# Puts every shop repo on a branch lesson-NN that starts at the tag lesson-NN-start.
# Refuses, changing nothing, when a tag is missing, a repo has uncommitted changes, or the
# branch lesson-NN already has commits after the tag. --discard throws those away instead
# and prints where the old branch was, so it can be brought back.
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
  branch_head=$(git -C "$repo" rev-parse -q --verify "refs/heads/lesson-$lesson" || true)
  tag_commit=$(git -C "$repo" rev-parse "$tag^{commit}")
  if [[ -n "$branch_head" && "$branch_head" != "$tag_commit" && "$discard" != "--discard" ]]; then
    echo "$repo: branch lesson-$lesson has commits after $tag (now at ${branch_head:0:7})." >&2
    echo "Rename the branch to keep them, or rerun with --discard to start the lesson again." >&2
    exit 1
  fi
done

for repo in "${repos[@]}"; do
  old_head=$(git -C "$repo" rev-parse -q --verify "refs/heads/lesson-$lesson" || true)
  git -C "$repo" checkout -q -f -B "lesson-$lesson" "$tag"
  if [[ -n "$old_head" && "$old_head" != "$(git -C "$repo" rev-parse HEAD)" ]]; then
    echo "$repo: lesson-$lesson was at ${old_head:0:7}. To get it back: git -C $repo branch my-lesson-$lesson ${old_head:0:7}"
  fi
  if [[ "$discard" == "--discard" ]]; then
    git -C "$repo" clean -q -fd
  fi
done
echo "Every repo is on branch lesson-$lesson, starting at $tag."
