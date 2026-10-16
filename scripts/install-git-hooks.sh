#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
existing=$(git config --get core.hooksPath || true)
if [[ -n "$existing" && "$existing" != '.githooks' ]]; then
    echo "Existing core.hooksPath=$existing; merge these hooks into your existing hooks before enabling them." >&2
    exit 1
fi
for hook in pre-commit pre-push; do
    if [[ -z "$existing" && -f "$(git rev-parse --git-path "hooks/$hook")" ]]; then
        echo "Existing $hook hook detected; integrate it with .githooks/$hook first." >&2
        exit 1
    fi
done
git config --local core.hooksPath .githooks
echo 'Enabled Java formatting checks for commits (staged files) and pushes (outgoing tips).'
