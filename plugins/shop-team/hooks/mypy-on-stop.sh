#!/usr/bin/env bash
# Stop hook: runs mypy over src when Claude finishes a turn.
# On errors it exits 2, which keeps Claude working and hands it mypy's output.
# Reads the hook JSON on stdin.
#
# Checks the session's own repo, and, for a session at the workspace root,
# every service repo or worktree below it with uncommitted changes under src.
# Skips a repo without src or without mypy in its pyproject.toml.
set -euo pipefail

# Already continuing because of this hook: let Claude stop rather than loop
# forever on errors it can't fix.
if [[ $(jq -r '.stop_hook_active // false') == "true" ]]; then
  exit 0
fi

has_mypy() {
  [[ -d "$1/src" ]] && grep -Eq '"mypy[^a-zA-Z0-9_-]' "$1/pyproject.toml" 2>/dev/null
}

root=$(cd "${CLAUDE_PROJECT_DIR:-.}" && pwd)
repos=()
has_mypy "$root" && repos+=("$root")
for dir in "$root"/*/; do
  dir=${dir%/}
  [[ -e "$dir/.git" ]] || continue
  has_mypy "$dir" || continue
  [[ -n $(git -C "$dir" status --porcelain -- src) ]] || continue
  repos+=("$dir")
done

failed=0
for repo in ${repos[@]+"${repos[@]}"}; do
  if ! output=$(cd "$repo" && uv run mypy src 2>&1); then
    printf 'uv run mypy src failed in %s. Fix these errors:\n%s\n' "$repo" "$output" >&2
    failed=1
  fi
done
[[ $failed -eq 0 ]] || exit 2
