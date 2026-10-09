#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export LOCAL_UID="$(id -u)" LOCAL_GID="$(id -g)"
mkdir -p results/benchmark
python3 scripts/generate.py --vertices "${VERTICES:-10000}" --per-user "${PER_USER:-8}"
docker compose build
read -ra core_counts <<< "${CORES:-1 2 4}"
for cores in "${core_counts[@]}"; do
  [[ "$cores" =~ ^[1-9][0-9]*$ ]] || { echo "Nombre de cœurs invalide" >&2; exit 1; }
  SPARK_MASTER="local[$cores]" docker compose run --rm graph \
    benchmark data/generated "results/benchmark/cores-$cores" "${REPEATS:-3}" "${ITERATIONS:-20}"
done
python3 scripts/summarize.py results/benchmark
