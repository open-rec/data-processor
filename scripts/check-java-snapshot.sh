#!/usr/bin/env bash
# Check the exact index/commit, including its formatter configuration, without
# changing the worktree or staging unrelated/partially staged edits.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
snapshot=$(mktemp -d "${TMPDIR:-/tmp}/data-processor-format.XXXXXXXX")
trap 'rm -rf -- "$snapshot"' EXIT
case "${1:-index}" in
    index) git checkout-index --all --prefix="$snapshot/" ;;
    *) git archive "$1" | tar -x -C "$snapshot" ;;
esac
# Git exports repository variables in hooks; do not let them point Spotless at
# the original repository while checking the isolated snapshot.
while IFS= read -r variable; do
    unset "$variable"
done < <(git rev-parse --local-env-vars)
if ! bash "$snapshot/scripts/java-format.sh" check; then
    echo 'Java format check failed. Run bash scripts/java-format.sh, review and stage the changes, then retry.' >&2
    exit 1
fi
