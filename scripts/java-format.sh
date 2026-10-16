#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
if [[ -n "${OPENREC_JAVA21_HOME:-}" ]]; then
    export JAVA_HOME="$OPENREC_JAVA21_HOME"
fi
if [[ -n "${JAVA_HOME:-}" ]]; then
    export PATH="$JAVA_HOME/bin:$PATH"
fi
case "${1:-apply}" in
    apply|check) goal="spotless:${1:-apply}" ;;
    *) echo "Usage: $0 [apply|check]" >&2; exit 2 ;;
esac
exec "${MVN:-mvn}" --batch-mode --no-transfer-progress "$goal"
