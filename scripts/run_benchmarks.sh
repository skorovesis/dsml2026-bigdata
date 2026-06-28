#!/bin/bash
# Run required benchmark configurations from the assignment.
set -eu

source ~/bigdata-env.sh
cd ~/dsml2026-project/code

USER_PATH="hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/${DSML_USER}"
OUT="${USER_PATH}/project-output"

submit() {
  local name="$1"
  shift
  echo "========== ${name} =========="
  spark-submit --py-files common.py --conf "spark.app.name=${name}" "$@" --output-base "$OUT"
}

echo "=== Requirement 1: convert to Parquet ==="
submit convert_parquet convert_parquet.py

echo "=== Requirement 1: format benchmark (Q2) ==="
spark-submit --py-files common.py --conf spark.app.name=format_benchmark benchmark_format.py

echo "=== Requirement 2: Q1 (2 executors, 1 core, 2g) ==="
for script in q1_rdd.py q1_df.py q1_df_udf.py; do
  submit "Q1_${script}" \
    --conf spark.executor.instances=2 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=2g \
    "$script"
done

echo "=== Requirement 3: Q2 (4 executors, 1 core, 2g) ==="
for script in q2_df.py q2_sql.py; do
  submit "Q2_${script}" \
    --conf spark.executor.instances=4 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=2g \
    "$script"
done

echo "=== Requirement 4: Q3 (3 executors, 1 core, 2g) ==="
for script in q3_df.py q3_rdd.py; do
  submit "Q3_${script}" \
    --conf spark.executor.instances=3 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=2g \
    "$script"
done

echo "=== Requirement 5A: Q4 vertical scaling ==="
for cfg in "1:2g" "2:4g" "4:8g"; do
  cores="${cfg%%:*}"
  mem="${cfg##*:}"
  submit "Q4_A_${cores}c_${mem}" \
    --conf spark.executor.instances=2 \
    --conf spark.executor.cores="${cores}" \
    --conf spark.executor.memory="${mem}" \
    q4_df.py
done

echo "=== Requirement 5B: Q4 horizontal scaling (8 cores, 16g total) ==="
submit Q4_B_2x4c_8g \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=4 \
  --conf spark.executor.memory=8g \
  q4_df.py

submit Q4_B_4x2c_4g \
  --conf spark.executor.instances=4 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=4g \
  q4_df.py

submit Q4_B_8x1c_2g \
  --conf spark.executor.instances=8 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  q4_df.py

echo "=== Requirement 6: join hints Q3/Q4 ==="
for hint in broadcast merge shuffle_hash shuffle_replicate_nl; do
  submit "Q3_hint_${hint}" \
    --conf spark.executor.instances=3 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=2g \
    q3_df.py --join-hint "$hint" --explain
  submit "Q4_hint_${hint}" \
    --conf spark.executor.instances=2 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=2g \
    q4_df.py --join-hint "$hint" --explain
done

echo "ALL_BENCHMARKS_SUBMITTED"
