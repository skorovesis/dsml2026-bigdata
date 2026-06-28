#!/bin/bash
# Generic spark-submit wrapper for DSML 2026 project scripts.
set -eu

SCRIPT_NAME="${1:?Usage: submit.sh <script.py> [extra args...]}"
shift

source ~/bigdata-env.sh
cd ~/dsml2026-project/code

USER_PATH="hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/${DSML_USER}"
OUTPUT_BASE="${USER_PATH}/project-output"

exec spark-submit --py-files common.py "$SCRIPT_NAME" --output-base "$OUTPUT_BASE" "$@"
