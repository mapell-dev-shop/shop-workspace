#!/usr/bin/env bash
# PreToolUse hook for Bash: blocks commands that destroy work or data.
# Exit 2 cancels the call and hands the reason on stderr back to Claude.
# Reads the hook JSON on stdin.
set -euo pipefail

command=$(jq -r '.tool_input.command // empty')

block() {
  echo "Blocked by the shop-team plugin (hooks/block-dangerous-bash.sh): $1" >&2
  echo "Don't work around this. If it is really needed, ask the user to run it themselves." >&2
  exit 2
}

# Check each simple command on its own, so flags from one don't count for another.
while IFS= read -r segment; do
  if grep -Eq '(^|[[:space:]])rm[[:space:]]' <<<"$segment" &&
    grep -Eq '[[:space:]](-[a-zA-Z]*[rR][a-zA-Z]*|--recursive)([[:space:]]|$)' <<<"$segment" &&
    grep -Eq '[[:space:]](-[a-zA-Z]*f[a-zA-Z]*|--force)([[:space:]]|$)' <<<"$segment"; then
    block "rm -rf deletes files recursively with no confirmation and no undo. Delete specific files, or ask the user."
  fi

  if grep -Eq '(^|[[:space:]])git[[:space:]](.*[[:space:]])?reset[[:space:]](.*[[:space:]])?--hard([[:space:]]|$)' <<<"$segment"; then
    block "git reset --hard throws away uncommitted changes for good. Use git stash, or git restore on specific files."
  fi

  if grep -Eq '(^|[[:space:]])git[[:space:]](.*[[:space:]])?push([[:space:]]|$)' <<<"$segment" &&
    grep -Eq '[[:space:]](-[a-zA-Z]*f[a-zA-Z]*|--force|--force-with-lease(=[^[:space:]]*)?|--force-if-includes)([[:space:]]|$)' <<<"$segment"; then
    block "git push --force rewrites the remote branch and can delete other people's commits. Push normally, or ask the user."
  fi

  if grep -Eq '(^|[[:space:]])alembic[[:space:]](.*[[:space:]])?downgrade([[:space:]]|$)' <<<"$segment"; then
    block "alembic downgrade drops columns or tables and the data in them. Write a new forward revision instead (see the repo's .claude/rules/migrations.md)."
  fi
done < <(tr ';&|(){}`\n' '\n\n\n\n\n\n\n\n' <<<"$command")

exit 0
