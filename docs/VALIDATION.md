# Validation de la livraison

Livraison préparée le 9 octobre 2026.

## Exécuté réellement

- Compilation du code Scala 2.12.18 avec les JAR Spark 3.5.3.
- Exécution avec Java 17.0.20 et Spark 3.5.3 en mode local.
- Commande `test` terminée avec succès : degrés, composantes, sommet isolé,
  comparaison PageRank avec une récurrence séquentielle indépendante,
  cycle symétrique, doublons, boucles et référence inconnue.
- Analyse de `data/demo` à 20 itérations terminée avec succès.
- Vérification des CSV : 20 identifiants distincts, degrés attendus de Hicham,
  Sara et Nour, somme des scores PageRank égale à 1 à la précision numérique,
  tailles des composantes 10, 5, 4 et 1.
- Générateur Python : reproductibilité à graine fixe, cardinalité attendue,
  absence de doublons, de boucles et de destinations hors intervalle.
- Benchmark PageRank réel à 1, 2 et 4 cœurs, 1 000 sommets, 8 000 liens,
  graine 42, 10 itérations, un échauffement puis trois répétitions par configuration.
- Génération du tableau de synthèse et du graphique depuis les mesures CSV.
- Syntaxe des scripts Bash, compilation syntaxique Python et lecture du YAML.

Les sorties sont fournies dans `examples/verified/`. Leurs durées décrivent
l'environnement de préparation, partagé et virtualisé, **pas le PC de Hicham**.
Elles servent d'exemple de protocole et ne sont pas une évaluation de matériel.
Les informations d'environnement disponibles sont dans `environment.json`.

## Non exécuté dans l'environnement de préparation

**Docker Engine n'y était pas disponible** : la construction de l'image et le lancement
avec Docker Compose ont ensuite été vérifiés sur le PC de l'auteur (section suivante). Le Dockerfile
utilise l'image officielle `apache/spark:3.5.3`, la même version Spark que le test natif.
Les fichiers de construction et scripts ont été inspectés, mais cela ne remplace
pas un lancement réel du conteneur.

Aucun déploiement multi-machine, aucun test de panne et aucune mesure à grande
échelle n'ont été réalisés. La campagne par défaut à 10 000 sommets doit être
lancée sur votre machine ; les mesures incluses portent sur 1 000 sommets.

## Exécuté sur le PC de l'auteur (9 octobre 2026)

Machine : PC portable, Intel Core i5-10310U (4 cœurs / 8 threads), 14 Gio de RAM,
Linux 7.0.0, Docker 29.9.0, Docker Compose v5.6.0. **Un seul hôte, pas de cluster.**
Profil d'énergie `balanced`, gouverneur CPU `powersave`. Les conditions de chaque
série de mesures sont indiquées ci-dessous. Détails : `examples/pc-local/environment.json`.

- `bash scripts/demo.sh` : construction de l'image, `TESTS OK`, analyse de `data/demo`
  terminées avec succès (code de sortie 0).
- Contrôle indépendant des CSV produits : 20 identifiants distincts, 29 liens
  (somme des abonnés = somme des abonnements = 29), 4 composantes de tailles
  10, 5, 4 et 1, Nour (20) isolée avec 0 abonné et 0 abonnement, Hicham 8 abonnés,
  Sara 6 abonnés, somme PageRank = 1.
- Benchmarks PageRank réels à 1, 2 et 4 cœurs (20 itérations, graine 42,
  1 échauffement + 3 répétitions) sur 10 000 sommets / 80 000 liens et
  100 000 sommets / 800 000 liens (`DRIVER_MEMORY=3g`), dans trois conditions
  successives. Médianes pour 1 / 2 / 4 cœurs :

  | Conditions | 10 000 sommets (s) | 100 000 sommets (s) | Dossier |
  |---|---|---|---|
  | Secteur, navigateurs fermés (référence) | 1,338 / 1,908 / 1,901 | 3,540 / 3,942 / 4,236 | `examples/pc-local/benchmark-*` |
  | Secteur, navigateurs ouverts | 1,248 / 1,755 / 1,974 | 3,711 / 3,887 / 4,234 | `examples/pc-local/comparaison/secteur-navigateurs-ouverts/` |
  | Batterie, navigateurs ouverts | 2,546 / 3,117 / 2,883 | 6,152 / 6,630 / 6,445 | `examples/pc-local/comparaison/batterie/` |

  Partitions = cœurs (1, 2, 4) dans toutes les séries. Aucune accélération observée ;
  l'alimentation sur secteur divise les durées par environ 2.

### Corrections apportées lors de ces essais

1. **Image Docker en Java 11.** `apache/spark:3.5.3` fournit Java 11.0.24, alors
   que la documentation annonce Java 17. Le Dockerfile utilise désormais
   `apache/spark:3.5.3-scala2.12-java17-ubuntu` (Java 17.0.12 mesuré).
2. **Benchmark limité à une partition.** Un CSV de petite taille est lu en une seule
   partition et `partitionBy` conservait ce nombre : PageRank s'exécutait sur une
   seule tâche quel que soit `local[N]` (colonne `partitions` = 1 partout). Le graphe
   est désormais réparti en `defaultParallelism` partitions (N en `local[N]`).
   Les mesures de `examples/verified/benchmark/` ont été obtenues **avant** cette
   correction (partitions = 1) et ne mesurent donc pas l'effet du parallélisme.

Après ces corrections, la démonstration et les deux benchmarks ont été relancés
avec succès ; seules les mesures postérieures aux corrections sont conservées.

## Refaire les vérifications

```bash
bash scripts/demo.sh
CORES='1 2 4' VERTICES=1000 REPEATS=3 ITERATIONS=10 bash scripts/benchmark.sh
```

Ou, avec une distribution Spark 3.5.3 locale :

```bash
export SPARK_HOME=/chemin/vers/spark
bash scripts/compile.sh
bash scripts/run-local.sh test
bash scripts/run-local.sh analyze data/demo results/demo 20
python3 scripts/generate.py --vertices 1000 --per-user 8 --seed 42
for cores in 1 2 4; do
  SPARK_MASTER="local[$cores]" bash scripts/run-local.sh benchmark data/generated "results/benchmark/cores-$cores" 3 10
done
python3 scripts/summarize.py results/benchmark
```
