#!/usr/bin/env bash
# Usage: ./checkout-lesson.sh NN [--end] [--discard]
# Puts every shop repo on a branch lesson-NN at the tag lesson-NN-start, or at lesson-NN-end
# with --end (to skip past a live demo that went wrong). Refuses, changing nothing, when a tag
# is missing, a repo has uncommitted changes, or the branch lesson-NN has commits the tag does
# not have. --discard throws those away instead and prints where the old branch was, so it can
# be brought back. The demo databases are removed every time: an earlier lesson's code cannot
# open a database that a later lesson migrated. ./run-all.sh builds them again.
set -euo pipefail
cd "$(dirname "$0")"

usage() {
  echo "usage: $0 NN [--end] [--discard]" >&2
  exit 2
}
[[ $# -ge 1 && "$1" =~ ^[0-9][0-9]$ ]] || usage
lesson=$1
shift
point=start
discard=
for arg in "$@"; do
  case $arg in
    --end) [[ $point == start ]] || usage; point=end ;;
    --discard) [[ -z $discard ]] || usage; discard=yes ;;
    *) usage ;;
  esac
done
tag="lesson-$lesson-$point"
repos=(. contracts orders-service inventory-service payments-service)

for repo in "${repos[@]}"; do
  if ! git -C "$repo" rev-parse -q --verify "refs/tags/$tag" > /dev/null; then
    echo "$repo: tag $tag not found" >&2
    exit 1
  fi
  if [[ -n "$(git -C "$repo" status --porcelain)" && -z $discard ]]; then
    echo "$repo has uncommitted changes. Commit them, or rerun with --discard." >&2
    exit 1
  fi
  branch_head=$(git -C "$repo" rev-parse -q --verify "refs/heads/lesson-$lesson" || true)
  if [[ -n $branch_head && -z $discard ]] && ! git -C "$repo" merge-base --is-ancestor "$branch_head" "$tag"; then
    echo "$repo: branch lesson-$lesson has commits that $tag does not have (now at ${branch_head:0:7})." >&2
    echo "Rename the branch to keep them, or rerun with --discard." >&2
    exit 1
  fi
done

for repo in "${repos[@]}"; do
  old_head=$(git -C "$repo" rev-parse -q --verify "refs/heads/lesson-$lesson" || true)
  git -C "$repo" checkout -q -f -B "lesson-$lesson" "$tag"
  if [[ -n $old_head ]] && ! git -C "$repo" merge-base --is-ancestor "$old_head" HEAD; then
    echo "$repo: lesson-$lesson was at ${old_head:0:7}. To get it back: git -C $repo branch my-lesson-$lesson ${old_head:0:7}"
  fi
  if [[ -n $discard ]]; then
    git -C "$repo" clean -q -fd
  fi
done

shopt -s nullglob
databases=(*-service/*.db)
if ((${#databases[@]})); then
  rm -- "${databases[@]}"
  echo "Removed ${databases[*]}. ./run-all.sh builds them again."
fi
echo "Every repo is on branch lesson-$lesson, at $tag."
