# Spark Social Graph

**Analyse d'un réseau social fictif avec Apache Spark GraphX et Scala.**

Ce projet transforme deux fichiers CSV en un graphe et répond à trois questions :
qui a le plus d'abonnés, qui possède une centralité PageRank élevée, et quels
utilisateurs sont reliés par un chemin lorsque l'on ignore le sens des liens ?

Il comprend un exemple de 20 utilisateurs, un générateur reproductible, des tests
et un protocole de mesure du temps de calcul avec 1, 2 ou 4 cœurs Spark.

**Deux versions dans ce dépôt :**

| Version | Dossier | Algorithmes |
|---|---|---|
| Scala + GraphX (principale) | racine du dépôt | `staticPageRank`, `connectedComponents` de GraphX |
| Python + PySpark | [`pyspark/`](pyspark/README.md) | écrits avec des DataFrames (GraphX n'a pas d'API Python) |

Les deux versions lisent les mêmes données et produisent les mêmes résultats.
Pour la version Python : `cd pyspark && bash scripts/demo.sh`.

## Démarrage rapide sur Ubuntu

Prérequis : **Docker Engine et Docker Compose v2** déjà installés et accessibles
à votre utilisateur. La première construction nécessite Internet. Prévoir environ
2 Go d'espace libre pour l'image et plusieurs Go de RAM disponibles. Aucun GPU requis.
Python 3 est nécessaire seulement pour générer des données et produire les graphiques.

Depuis le dossier du projet :

```bash
bash scripts/demo.sh
```

Cette commande construit l'image, exécute les tests, puis analyse l'exemple.
Elle peut prendre quelques minutes au premier lancement.

Lire ensuite **`results/demo/rapport.md`**. Les données complètes sont dans
`results/demo/users/part-*.csv` et `results/demo/components/part-*.csv`.
Spark produit des répertoires de fichiers CSV : c'est normal pour un traitement partitionné.
Les relances remplacent uniquement les sorties de l'analyse sélectionnée.

Deux jeux de résultats sont fournis et ne doivent pas être confondus :

- **`examples/pc-local/`** : exécution réelle avec Docker sur le PC portable de l'auteur
  (Intel Core i5-10310U, 4 cœurs / 8 threads, 14 Gio, un seul hôte) le 9 octobre 2026.
- **`examples/verified/`** : exemple produit dans l'environnement de préparation initial
  (Spark natif, sans Docker), avant la correction du partitionnement décrite dans
  `docs/VALIDATION.md`. Ses durées n'appartiennent pas à votre ordinateur.

## Ce que fait le programme

1. Charger et valider `users.csv` et `follows.csv`.
2. Retirer les liens répétés et les abonnements à soi-même.
3. Construire le graphe, en conservant les utilisateurs isolés.
4. Calculer les degrés entrants et sortants, PageRank et les composantes connexes.
5. Écrire les CSV et un compte rendu avec les 10 premiers scores PageRank.

| Résultat | Signification |
|---|---|
| `followers` | Nombre d'utilisateurs distincts qui suivent ce compte |
| `following` | Nombre de comptes distincts suivis |
| `pagerank` | Centralité issue des liens entrants et de leur provenance ; somme des scores = 1 |
| `component` | Plus petit identifiant dans la composante connexe, sens des liens ignoré |

Sur l'exemple : **20 utilisateurs, 29 liens, 4 composantes** de tailles 10, 5, 4 et 1.
Nour (20) est isolée et doit apparaître dans les sorties avec zéro abonné et zéro abonnement.

## Technologies et portée

- Apache Spark **3.5.3**, GraphX, Scala **2.12** et Java **17**
  (image officielle `apache/spark:3.5.3-scala2.12-java17-ubuntu`).
- Docker pour fournir le même environnement logiciel.
- Python 3, bibliothèque standard uniquement, pour génération et synthèse des mesures.

Les commandes fournies utilisent **le mode local multicœur sur un seul ordinateur**.
GraphX est conçu pour le calcul distribué, mais ce projet ne prétend pas avoir été
mesuré sur un cluster de plusieurs machines. Plusieurs conteneurs sur un seul PC
ne démontreraient pas non plus une accélération multi-machine.

Les versions sont fixées pour la reproductibilité de cette livraison ; ce n'est pas
une recommandation de dernière version ou une configuration de service en production.

## Autres commandes Docker

Les scripts `demo.sh` et `benchmark.sh` transmettent automatiquement votre UID/GID,
afin que les sorties vous appartiennent. Pour lancer directement les commandes :

```bash
export LOCAL_UID=$(id -u)
export LOCAL_GID=$(id -g)
mkdir -p results
docker compose build

# Vérifier les algorithmes et le chargement des données
docker compose run --rm graph test

# Refaire l'analyse avec 30 itérations PageRank
docker compose run --rm graph analyze data/demo results/demo 30

# Utiliser un seul cœur Spark
SPARK_MASTER='local[1]' docker compose run --rm graph analyze data/demo results/one-core 20
```

## Comparer les performances

```bash
bash scripts/benchmark.sh
```

Par défaut : 10 000 utilisateurs, 80 000 liens, 20 itérations PageRank,
1 échauffement puis 3 répétitions à 1, 2 et 4 cœurs.
Le script reconstruit le même graphe avec la graine 42.

```bash
# Configuration plus courte, utile pour un premier essai
CORES='1 2' VERTICES=1000 REPEATS=2 ITERATIONS=10 bash scripts/benchmark.sh
```

Résultats : `results/benchmark/rapport.md`, `timings.svg` et les mesures brutes
`cores-N/timings.csv`. Le graphique contient exclusivement vos durées mesurées.
Avant une nouvelle campagne avec des paramètres différents, déplacer l'ancien
dossier `results/benchmark` ailleurs pour ne pas mélanger les séries.

Le chronomètre couvre PageRank et l'action qui force le calcul, graphe déjà chargé.
Il exclut le démarrage de Spark, la lecture initiale et l'écriture des résultats.
Les benchmarks ne mesurent ni les composantes connexes ni l'ensemble du pipeline.
Le parallélisme et le partitionnement par défaut peuvent varier ensemble.
Les threads auxiliaires de la JVM ne sont pas limités par `local[N]`.

Le graphe est réparti en autant de partitions d'arêtes que de cœurs Spark
(colonne `partitions` des fichiers `timings.csv`).

**Aucune accélération n'est garantie** : les petits graphes peuvent être plus lents
avec davantage de cœurs. Documenter CPU, RAM, système, mode d'alimentation et charge
de la machine ; répéter les campagnes et examiner la dispersion.

### Mesures réelles sur le PC de l'auteur

Docker Compose, mode `local[N]` dans un conteneur sur **un seul ordinateur** (pas de cluster).
Intel Core i5-10310U (4 cœurs / 8 threads), 14 Gio de RAM, Linux, profil d'énergie `balanced`.
PageRank 20 itérations, graine 42, 1 échauffement puis 3 répétitions ; médianes en secondes
(accélération par rapport à 1 cœur entre parenthèses).

**Série de référence : sur secteur, navigateurs fermés** (charge moyenne ≈ 1 au départ) :

| Graphe | 1 cœur | 2 cœurs | 4 cœurs |
|---|---|---|---|
| 10 000 sommets, 80 000 liens | 1,338 | 1,908 (×0,70) | 1,901 (×0,70) |
| 100 000 sommets, 800 000 liens | 3,540 | 3,942 (×0,90) | 4,236 (×0,84) |

Comparaison avec les autres conditions mesurées le même soir sur ce PC :

| Graphe | Conditions | 1 cœur | 2 cœurs | 4 cœurs |
|---|---|---|---|---|
| 10 000 sommets | secteur, navigateurs ouverts | 1,248 | 1,755 | 1,974 |
| 10 000 sommets | batterie, navigateurs ouverts | 2,546 | 3,117 | 2,883 |
| 100 000 sommets | secteur, navigateurs ouverts | 3,711 | 3,887 | 4,234 |
| 100 000 sommets | batterie, navigateurs ouverts | 6,152 | 6,630 | 6,445 |

Sur batterie, les durées sont environ deux fois plus longues ; fermer les navigateurs
ne change presque rien. Dans toutes les conditions, **ajouter des cœurs n'a pas accéléré
PageRank** à ces tailles : le coût de coordination et d'échange entre partitions domine.
Détails : `examples/pc-local/benchmark-*/` (référence) et `examples/pc-local/comparaison/`.

Commandes exécutées :

```bash
CORES='1 2 4' VERTICES=10000 REPEATS=3 ITERATIONS=20 bash scripts/benchmark.sh
mv results/benchmark results/benchmark-10k
DRIVER_MEMORY=3g CORES='1 2 4' VERTICES=100000 REPEATS=3 ITERATIONS=20 bash scripts/benchmark.sh
```

## Organisation

- `src/main/scala/SocialGraph.scala` : application et tests d'intégration.
- `data/demo/` : les deux CSV fictifs, sans données personnelles réelles.
- `scripts/` : compilation, lancement, génération, mesures et graphique.
- `docs/COMPRENDRE.md` : explications progressives et préparation à l'entretien.
- `docs/VALIDATION.md` : ce qui a réellement été exécuté pour cette livraison.
- `examples/pc-local/` : résultats réels obtenus sur le PC de l'auteur (Docker, un seul hôte).
- `examples/verified/` : exemple de l'environnement de préparation initial.
- `Dockerfile`, `compose.yaml` : environnement reproductible.
- `pyspark/` : la même analyse en Python avec PySpark (voir son README).

## Sans Docker, avec Spark installé

Java 17, une distribution Spark 3.5.3 Hadoop 3 contenant ses JAR Scala et `jar`
(fourni par un JDK), ou Python 3 en remplacement de `jar`, sont nécessaires.
Définir le chemin réel de Spark :

```bash
export SPARK_HOME=/chemin/vers/spark
bash scripts/compile.sh
bash scripts/run-local.sh test
bash scripts/run-local.sh analyze data/demo results/demo 20
```

La compilation utilise le compilateur Scala déjà livré avec Spark : aucun téléchargement
SBT/Maven additionnel n'est nécessaire. Les fichiers `target/` sont reconstruits.

## Mettre ce projet dans votre dépôt existant

Copier les fichiers de cette archive dans votre **copie locale** du dépôt en conservant
son dossier `.git`. Faire d'abord une copie de sauvegarde du README existant.
Ne pas copier de résultats personnels ou de grands jeux de données.

```bash
git status
git add README.md Dockerfile compose.yaml .gitignore .dockerignore src scripts docs data/demo examples results/.gitkeep
git commit -m "Add Spark GraphX social network analysis and benchmarks"
git push origin main
```

Vérifier les fichiers affichés par `git status` avant le commit.
Ces commandes supposent que vous êtes dans le bon dépôt, sur `main` et que son
remote est déjà configuré. Cette archive ne modifie pas votre GitHub automatiquement.

## Limites et améliorations possibles

- Données fictives : aucune conclusion sur un réseau social réel.
- PageRank à nombre d'itérations fixe : 20 itérations ne prouvent pas la convergence.
- La convention GraphX pour les sommets sans sortie est explicitée dans le guide.
- Une composante connexe n'est pas une communauté sociologique.
- Pas de scraping, API externe, interface web ou service permanent.
- Extension utile : jeu public documenté, arrêt sur tolérance, puis cluster réellement
  multi-machine avec stockage partagé. Les rapports Markdown actuels sont écrits par
  le driver sur son système de fichiers local ; leur collecte doit être adaptée au cluster.

## Références

- [Présentation officielle GraphX](https://spark.apache.org/graphx/)
- [Guide GraphX, branche Spark 3.5](https://spark.apache.org/docs/3.5.8/graphx-programming-guide.html)
- [Sources Apache Spark 3.5.3](https://github.com/apache/spark/tree/v3.5.3)
- [Images Docker Apache Spark](https://github.com/apache/spark-docker)

Les algorithmes de graphe sont ceux de GraphX. La contribution de ce projet est leur
intégration dans un pipeline validé, reproductible et mesurable.
