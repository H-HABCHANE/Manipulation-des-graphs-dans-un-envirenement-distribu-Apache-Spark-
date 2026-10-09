#!/usr/bin/env python3
"""Médianes, accélérations et graphique SVG depuis des mesures réelles."""
import csv
import html
import re
import statistics
import sys
from pathlib import Path


def summarize(root):
    measurements = []
    settings = set()
    for file in sorted(root.glob("cores-*/timings.csv")):
        with file.open(encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        if not rows:
            raise ValueError(f"Fichier vide : {file}")
        master = rows[0]["master"]
        match = re.fullmatch(r"local\[(\d+)\]", master)
        if not match or any(r["master"] != master for r in rows):
            raise ValueError("Configurations incohérentes")
        for r in rows:
            settings.add(tuple(r[k] for k in ["vertices", "edges", "iterations", "spark_version", "java_version"]))
        values = [float(r["seconds"]) for r in rows]
        if any(v <= 0 for v in values):
            raise ValueError("Les durées doivent être positives")
        measurements.append((int(match[1]), statistics.median(values), min(values), max(values), len(values)))
    if not measurements or len(settings) != 1:
        raise ValueError("Mesures absentes ou données / versions / itérations différentes")
    measurements.sort()
    if len({r[0] for r in measurements}) != len(measurements):
        raise ValueError("Plusieurs fichiers pour la même configuration")
    baseline = next((r[1] for r in measurements if r[0] == 1), None)
    if baseline is None:
        raise ValueError("La mesure à un cœur est nécessaire")
    settings = next(iter(settings))
    text = ["# Comparaison PageRank", "", f"{settings[0]} sommets, {settings[1]} liens, {settings[2]} itérations.",
            f"Spark {settings[3]} ; Java {settings[4]}. Un échauffement avant chaque série.", "",
            "| Cœurs Spark | Médiane (s) | Min (s) | Max (s) | Répétitions | Accélération | Efficacité |",
            "|---|---|---|---|---|---|---|"]
    for cores, median, low, high, count in measurements:
        speedup = baseline / median
        text.append(f"| {cores} | {median:.3f} | {low:.3f} | {high:.3f} | {count} | {speedup:.2f} | {100*speedup/cores:.1f}% |")
    text += ["", "Accélération = médiane à 1 cœur / médiane à p cœurs.",
             "Efficacité = accélération / p. Les cœurs désignent les threads de calcul autorisés à Spark.",
             "Mesures locales sur un seul hôte, graphe déjà chargé. Le démarrage JVM et les exports sont exclus.",
             "Ce protocole fait varier le parallélisme et le partitionnement par défaut de Spark ; ce n'est pas un test à partitionnement fixe.",
             "Le coût de coordination peut dépasser le gain sur de petits graphes. Une accélération n'est pas garantie.",
             "Pour comparer deux machines, conserver le même jeu de données et documenter CPU, RAM, système et charge.",
             "", "![Durées mesurées](timings.svg)", ""]
    (root / "rapport.md").write_text("\n".join(text), encoding="utf-8")
    width, height = 800, 130 + 64 * len(measurements)
    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
           '<rect width="100%" height="100%" fill="#f5f7fb"/>',
           '<g font-family="sans-serif" fill="#152741">',
           '<text x="32" y="38" font-size="24">PageRank — durées médianes mesurées</text>',
           '<text x="32" y="63" font-size="14">Plus court = plus rapide · exécution locale multicœur</text>']
    maximum = max(r[1] for r in measurements)
    for i, (cores, median, *_rest) in enumerate(measurements):
        y = 95 + i * 64
        bar_width = 480 * median / maximum
        svg.extend([f'<text x="32" y="{y+23}">{cores} cœur(s)</text>',
                    f'<rect x="145" y="{y}" width="{bar_width:.1f}" height="34" rx="6" fill="#176baf"/>',
                    f'<text x="{155+bar_width:.1f}" y="{y+23}">{html.escape(f"{median:.3f} s")}</text>'])
    svg.append('</g></svg>')
    (root / "timings.svg").write_text("\n".join(svg), encoding="utf-8")
    print("\n".join(text))


if __name__ == "__main__":
    summarize(Path(sys.argv[1] if len(sys.argv) > 1 else "results/benchmark"))
