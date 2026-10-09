#!/usr/bin/env python3
"""Graphe fictif reproductible, sans dépendances Python externes."""
import argparse
import csv
import random
from pathlib import Path


def generate(destination, vertices, per_user, seed):
    if vertices < 2 or not 1 <= per_user < vertices:
        raise ValueError("Il faut vertices >= 2 et 1 <= per_user < vertices")
    rng = random.Random(seed)
    destination.mkdir(parents=True, exist_ok=True)
    with (destination / "users.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name"])
        writer.writerows((i, f"User_{i:06d}") for i in range(1, vertices + 1))
    with (destination / "follows.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["src", "dst"])
        for src in range(1, vertices + 1):
            # Échantillonnage sans remise ; sauter l'identifiant source.
            targets = rng.sample(range(1, vertices), per_user)
            writer.writerows((src, t + (t >= src)) for t in sorted(targets))
    print(f"{vertices} utilisateurs ; {vertices * per_user} liens ; graine {seed} ; {destination}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", type=Path, default=Path("data/generated"))
    p.add_argument("--vertices", type=int, default=10000)
    p.add_argument("--per-user", type=int, default=8)
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args()
    generate(a.output, a.vertices, a.per_user, a.seed)
