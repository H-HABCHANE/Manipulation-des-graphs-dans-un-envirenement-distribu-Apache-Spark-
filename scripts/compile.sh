#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${SPARK_HOME:=/opt/spark}"
mkdir -p target/classes
java -Dscala.usejavacp=true -cp "$SPARK_HOME/jars/*" scala.tools.nsc.Main \
  -classpath "$SPARK_HOME/jars/*" -d target/classes src/main/scala/SocialGraph.scala
if command -v jar >/dev/null 2>&1; then
  jar cf target/social-graph.jar -C target/classes .
else
  # Certaines distributions fournissent java et les JAR Scala, mais pas l'outil jar.
  python3 - <<'PY'
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
root = Path('target/classes')
with ZipFile('target/social-graph.jar', 'w', ZIP_DEFLATED) as out:
    for file in sorted(root.rglob('*.class')):
        out.write(file, file.relative_to(root))
PY
fi
echo "Compilation terminée : target/social-graph.jar"
