#!/bin/bash
# src/benchmark/run_benchmark.sh
NDJSON="data/farmers-protest-tweets-2021-2-4.json"
N=10
FUNCS=("top_active_dates" "top_emojis" "top_mentioned_users")
BACKENDS=("pandas" "polars" "duckdb" "ijson")

for B in "${BACKENDS[@]}"; do
  for F in "${FUNCS[@]}"; do
    echo ">> Backend: $B | Func: $F"
    python src/benchmark/benchmark_script.py \
      --backend "$B" \
      --func "$F" \
      --ndjson "$NDJSON" \
      --n $N \
      --save-cprofile
  done
done
echo "=== Benchmark completo. Revisa profile_reports/master_summary.csv ==="
