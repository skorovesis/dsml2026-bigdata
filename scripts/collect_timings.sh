#!/bin/bash
# Export timings as LaTeX rows and plain text summary.
set -eu
source ~/bigdata-env.sh
NS=dsml00297-priv
OUT=~/dsml2026-project/timings_collected.txt

{
  echo "# Collected $(date -Iseconds)"
  kubectl -n "$NS" get pods -o name | grep driver | while read -r podref; do
    pod="${podref#pod/}"
    status=$(kubectl -n "$NS" get pod "$pod" -o jsonpath='{.status.phase}')
    times=$(kubectl -n "$NS" logs "$pod" 2>/dev/null | grep -E 'QUERY_ELAPSED|CONVERT_ELAPSED|SUMMARY_' || true)
    echo "POD=$pod STATUS=$status"
    echo "$times"
    echo "---"
  done
} | tee "$OUT"

echo ""
echo "Done. See $OUT"
