#!/usr/bin/env bash
set -euo pipefail
cd /app
if [[ "${1:-}" == "test" ]]; then
  # Py4J laisse des sockets ouvertes à la fermeture : avertissement sans conséquence.
  exec python3 -W ignore::ResourceWarning -m unittest discover -s tests -v
fi
exec python3 src/social_graph.py "$@"
