#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export LOCAL_UID="$(id -u)" LOCAL_GID="$(id -g)"
mkdir -p results
docker compose build
docker compose run --rm graph test
docker compose run --rm graph analyze data/demo results/demo 20
echo "Résultats : results/demo/rapport.md"
