#!/usr/bin/env bash
# PostToolUse hook for Edit and Write: runs ruff format on the edited file,
# with the ruff and settings of the git repository the file belongs to.
# Reads the hook JSON on stdin. Skips anything that isn't a Python file, and
# repos whose pyproject.toml doesn't list ruff (like the workspace root).
set -euo pipefail

file=$(jq -r '.tool_input.file_path // empty')
case "$file" in
  *.py | *.pyi) ;;
  *) exit 0 ;;
esac
[[ -f "$file" ]] || exit 0

repo=$(git -C "$(dirname "$file")" rev-parse --show-toplevel 2>/dev/null) || exit 0
grep -Eq '"ruff[^a-zA-Z0-9_-]' "$repo/pyproject.toml" 2>/dev/null || exit 0

cd "$repo"
uv run ruff format --quiet "$file" >&2
