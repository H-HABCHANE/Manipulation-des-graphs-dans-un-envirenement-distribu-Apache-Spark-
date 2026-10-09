# Spark Social Graph — version PySpark

**Le même projet que la version Scala/GraphX, réécrit en Python avec PySpark.**

Le programme lit les mêmes fichiers CSV (`../data/demo/`) et calcule les mêmes résultats :
abonnés et abonnements, PageRank (influence) et composantes connexes (groupes reliés).
La version Scala, à la racine du dépôt, reste inchangée.

## Différence essentielle avec la version Scala

**GraphX n'existe pas en Python.** Les algorithmes sont donc écrits avec des
**DataFrames Spark** (jointures et agrégations), dans `src/social_graph.py` :

| Calcul | Version Scala | Version PySpark |
|---|---|---|
| Degrés | `graph.inDegrees` / `outDegrees` | `groupBy("dst").count()` / `groupBy("src").count()` |
| PageRank | `graph.staticPageRank(20)` | boucle de 20 itérations : jointure liens × rangs, puis somme par destination |
| Composantes connexes | `graph.connectedComponents()` | propagation du plus petit identifiant entre voisins jusqu'à stabilisation |

PageRank suit exactement la récurrence de GraphX :
`rang(v) = 0,15 + 0,85 × Σ rang(u) / sortants(u)` sur les liens `u → v`, en partant de 1,
puis normalisation pour que la somme fasse 1. Les sommets sans lien sortant ne redistribuent
pas leur rang (même convention que GraphX).

Le nombre de partitions est égal au nombre de cœurs (`local[N]` → N partitions), comme
dans la version Scala corrigée. L'optimisation adaptative de Spark (AQE) est désactivée
pour que ce nombre reste fixe pendant les mesures.

Une alternative serait la bibliothèque externe **GraphFrames**, qui fournit PageRank et
les composantes en Python. Elle n'a pas été retenue : elle ajoute un téléchargement de
paquet au démarrage, et l'écriture explicite des algorithmes montre ce que fait Spark.

## Lancer le projet

Depuis la racine du dépôt :

```bash
cd pyspark
bash scripts/demo.sh            # construit l'image, lance les tests, analyse les 20 utilisateurs
cat results/demo/rapport.md
```

Commandes séparées :

```bash
cd pyspark
export LOCAL_UID=$(id -u) LOCAL_GID=$(id -g)
docker compose build
docker compose run --rm graph test
docker compose run --rm graph analyze data/demo results/demo 20
SPARK_MASTER='local[1]' docker compose run --rm graph analyze data/demo results/un-coeur 20
```

Benchmark (mêmes paramètres et même graphe généré, graine 42, que la version Scala) :

```bash
cd pyspark
mv results/benchmark results/benchmark-ancien-$(date +%H%M) 2>/dev/null
CORES='1 2 4' VERTICES=10000 REPEATS=3 ITERATIONS=20 bash scripts/benchmark.sh
cat results/benchmark/rapport.md
```

Le script réutilise `../scripts/generate.py` et `../scripts/summarize.py`.

## Organisation

- `src/social_graph.py` : chargement, validation, degrés, PageRank, composantes, analyse, benchmark.
- `tests/test_social_graph.py` : tests `unittest` (bibliothèque standard).
- `scripts/` : `demo.sh`, `benchmark.sh` et `run.sh` (point d'entrée du conteneur).
- `Dockerfile`, `compose.yaml` : image officielle `apache/spark:3.5.3-scala2.12-java17-python3-ubuntu`.
- `examples/pc-local/` : résultats réels obtenus sur le PC de l'auteur.
- `results/` : sorties de vos exécutions (ignorées par Git).

## Tests

`docker compose run --rm graph test` exécute 4 tests :

1. Degrés, composantes et sommet isolé sur un petit graphe ; PageRank comparé à une
   récurrence séquentielle écrite en Python pur ; somme des scores égale à 1.
2. Cycle symétrique : les trois scores valent 1/3.
3. Graphe aléatoire de 60 sommets (dont 10 isolés) : PageRank comparé à la référence
   Python et composantes comparées à un algorithme union-find indépendant.
4. Chargement : doublons et auto-abonnements retirés, utilisateur isolé conservé,
   référence inconnue et identifiant dupliqué rejetés.

## Validation sur le PC de l'auteur (10 octobre 2026)

PC portable Intel Core i5-10310U (4 cœurs / 8 threads), 14 Gio de RAM, Linux, Docker 29.9.0.
Spark 3.5.3, Java 17.0.12, Python 3.10.12. **Un seul hôte, pas de cluster.**
Conditions : sur secteur, navigateur ouvert, profil d'énergie `balanced`.

- `bash scripts/demo.sh` : construction, `Ran 4 tests ... OK`, analyse terminée (code 0).
- 20 utilisateurs, 29 liens, 4 composantes de tailles 10, 5, 4 et 1.
- **Comparaison avec la version Scala/GraphX** sur `data/demo` : noms, abonnés, abonnements
  et composantes identiques pour les 20 utilisateurs ; écart maximal de PageRank 2,8 × 10⁻¹⁷
  (arrondi en virgule flottante).

Benchmark PageRank, 10 000 sommets / 80 000 liens, 20 itérations, 1 échauffement puis
3 répétitions ; médianes en secondes. La version Scala a été relancée juste après,
dans les mêmes conditions :

| Version | 1 cœur | 2 cœurs | 4 cœurs |
|---|---|---|---|
| PySpark (DataFrames) | 4,049 | 3,775 (×1,07) | 4,027 (×1,01) |
| Scala (GraphX) | 2,211 | 1,719 (×1,29) | 3,361 (×0,66) |

Détails : `examples/pc-local/benchmark-10k/` et
`examples/pc-local/comparaison-scala-memes-conditions/benchmark-10k/`.

À retenir :

- **GraphX est environ 1,5 à 2 fois plus rapide** ici : il est spécialisé pour les graphes,
  alors que la version PySpark enchaîne des jointures de DataFrames génériques et
  matérialise les rangs à chaque itération (`localCheckpoint`).
- Le code Python ne fait que **piloter** Spark : les calculs s'exécutent dans la JVM.
  Aucune fonction Python (UDF) n'est appliquée ligne par ligne.
- Comme pour la version Scala, ajouter des cœurs n'apporte **pas d'accélération nette**
  à cette taille de graphe. Avec un navigateur ouvert, les mesures varient davantage :
  la série Scala sur secteur avec navigateurs fermés (racine du dépôt) donnait
  1,338 / 1,908 / 1,901 s.

## Limites

- Mêmes limites que la version Scala : données fictives, nombre d'itérations fixe,
  exécution locale sur une seule machine.
- Les composantes connexes par propagation demandent autant d'itérations que le plus
  long chemin du graphe : c'est plus lent que l'implémentation GraphX sur de grands graphes.
- Le benchmark ne mesure que PageRank, comme la version Scala.
