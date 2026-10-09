#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${SPARK_HOME:=/opt/spark}"
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
exec "$SPARK_HOME/bin/spark-submit" --master "${SPARK_MASTER:-local[2]}" \
  --driver-memory "${DRIVER_MEMORY:-1g}" --conf spark.ui.enabled=false \
  --conf spark.sql.shuffle.partitions=4 --class SocialGraph target/social-graph.jar "$@"
