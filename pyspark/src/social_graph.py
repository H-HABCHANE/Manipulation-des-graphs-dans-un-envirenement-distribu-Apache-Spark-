#!/usr/bin/env python3
"""Analyse d'un réseau social avec PySpark : degrés, PageRank et composantes connexes.

GraphX n'a pas d'API Python. Les algorithmes sont donc écrits avec des DataFrames
Spark (jointures et agrégations), en suivant la même définition que la version Scala :
un lien source -> destination signifie « source suit destination ».

Usage :
    social_graph.py analyze DATA OUTPUT [iterations]
    social_graph.py benchmark DATA OUTPUT [repetitions] [iterations]
"""
import os
import sys
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

USERS_SCHEMA = StructType([StructField("id", LongType()), StructField("name", StringType())])
LINKS_SCHEMA = StructType([StructField("src", LongType()), StructField("dst", LongType())])
RESET_PROBABILITY = 0.15


def create_spark(app_name="PySpark Social Graph"):
    """Session locale ; une partition de shuffle par cœur, comme la version Scala."""
    spark = (SparkSession.builder.appName(app_name)
             .master(os.environ.get("SPARK_MASTER", "local[2]"))
             .config("spark.driver.memory", os.environ.get("DRIVER_MEMORY", "1g"))
             .config("spark.ui.enabled", "false")
             .config("spark.ui.showConsoleProgress", "false")
             # Partitionnement fixe et prévisible : pas de fusion automatique des partitions.
             .config("spark.sql.adaptive.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", str(spark.sparkContext.defaultParallelism))
    return spark


def _partitions(spark):
    return spark.sparkContext.defaultParallelism


def load(spark, directory):
    """Lit et valide users.csv / follows.csv ; retourne (sommets, liens nettoyés) en cache."""
    def csv(name, schema):
        return (spark.read.option("header", "true").option("mode", "FAILFAST")
                .option("enforceSchema", "false").schema(schema).csv(f"{directory}/{name}"))

    users = csv("users.csv", USERS_SCHEMA).cache()
    links = csv("follows.csv", LINKS_SCHEMA).cache()
    try:
        if users.count() == 0:
            raise ValueError("Le fichier users.csv doit contenir au moins un utilisateur.")
        if users.filter("id IS NULL OR name IS NULL OR trim(name) = ''").count() > 0:
            raise ValueError("Chaque utilisateur doit avoir un identifiant entier et un nom non vide.")
        if users.groupBy("id").count().filter("count > 1").count() > 0:
            raise ValueError("Identifiants utilisateurs dupliqués.")
        if links.filter("src IS NULL OR dst IS NULL").count() > 0:
            raise ValueError("Chaque lien doit avoir deux identifiants entiers.")
        endpoints = links.select(F.col("src").alias("id")).union(links.select(F.col("dst").alias("id")))
        if endpoints.join(users, "id", "left_anti").count() > 0:
            raise ValueError("Un lien référence un utilisateur absent de users.csv.")
        n = _partitions(spark)
        vertices = users.repartition(n, "id").cache()
        # Un abonnement à soi-même est ignoré ; un abonnement répété ne compte qu'une fois.
        edges = links.filter("src <> dst").distinct().repartition(n, "src").cache()
        vertices.count()
        edges.count()
        return vertices, edges
    finally:
        users.unpersist()
        links.unpersist()


def pagerank(vertices, edges, iterations, reset=RESET_PROBABILITY):
    """PageRank statique, même récurrence que GraphX staticPageRank :
    rang(v) = reset + (1 - reset) * somme(rang(u) / sortants(u)) sur les liens u -> v.
    Les sommets sans lien sortant ne redistribuent pas leur rang (convention GraphX).
    Retourne les rangs bruts (non normalisés)."""
    out_degrees = edges.groupBy("src").agg(F.count("*").alias("out"))
    weighted = (edges.join(out_degrees, "src")
                .select("src", "dst", (F.lit(1.0) / F.col("out")).alias("weight")).cache())
    ids = vertices.select("id").cache()
    ranks = ids.withColumn("rank", F.lit(1.0))
    for _ in range(iterations):
        contributions = (weighted.join(ranks.withColumnRenamed("id", "src"), "src")
                         .groupBy("dst").agg(F.sum(F.col("rank") * F.col("weight")).alias("total")))
        ranks = (ids.join(contributions.withColumnRenamed("dst", "id"), "id", "left")
                 .select("id", (F.lit(reset) + F.lit(1.0 - reset) * F.coalesce("total", F.lit(0.0)))
                         .alias("rank")))
        # Couper la lignée : sinon le plan grossit à chaque itération.
        ranks = ranks.localCheckpoint(eager=True)
    weighted.unpersist()
    ids.unpersist()
    return ranks


def connected_components(vertices, edges, max_iterations=None):
    """Composantes connexes, sens des liens ignoré : chaque sommet adopte le plus petit
    identifiant parmi ses voisins jusqu'à stabilisation (propagation d'étiquettes)."""
    undirected = (edges.select("src", "dst")
                  .union(edges.select(F.col("dst").alias("src"), F.col("src").alias("dst")))
                  .distinct().cache())
    labels = vertices.select("id", F.col("id").alias("component")).localCheckpoint(eager=True)
    limit = max_iterations or vertices.count()
    for _ in range(limit):
        smallest = (undirected.join(labels.withColumnRenamed("id", "src"), "src")
                    .groupBy("dst").agg(F.min("component").alias("candidate")))
        updated = (labels.join(smallest.withColumnRenamed("dst", "id"), "id", "left")
                   .select("id", F.least("component", F.coalesce("candidate", "component"))
                           .alias("component"))
                   .localCheckpoint(eager=True))
        changed = (updated.alias("new").join(labels.alias("old"), "id")
                   .filter(F.col("new.component") != F.col("old.component")).count())
        labels = updated
        if changed == 0:
            break
    undirected.unpersist()
    return labels


def metrics(vertices, edges, iterations):
    """Une ligne par utilisateur, y compris les isolés : id, name, followers, following,
    pagerank (normalisé, somme = 1), component."""
    followers = edges.groupBy(F.col("dst").alias("id")).agg(F.count("*").alias("followers"))
    following = edges.groupBy(F.col("src").alias("id")).agg(F.count("*").alias("following"))
    ranks = pagerank(vertices, edges, iterations)
    total = ranks.agg(F.sum("rank")).first()[0]
    components = connected_components(vertices, edges)
    return (vertices.join(followers, "id", "left").join(following, "id", "left")
            .join(ranks, "id").join(components, "id")
            .select("id", "name",
                    F.coalesce("followers", F.lit(0)).cast("int").alias("followers"),
                    F.coalesce("following", F.lit(0)).cast("int").alias("following"),
                    (F.col("rank") / F.lit(total)).alias("pagerank"),
                    "component"))


def analyze(spark, data, output, iterations):
    vertices, edges = load(spark, data)
    n, m = vertices.count(), edges.count()
    start = time.perf_counter()
    rows = metrics(vertices, edges, iterations).cache()
    rows.count()  # Spark est paresseux : déclencher réellement tous les calculs.
    seconds = time.perf_counter() - start
    rows.write.mode("overwrite").option("header", "true").csv(f"{output}/users")
    groups = rows.groupBy("component").count().orderBy(F.desc("count"), "component")
    groups.write.mode("overwrite").option("header", "true").csv(f"{output}/components")
    top = rows.orderBy(F.desc("pagerank"), "id").limit(10).collect()
    table = "\n".join(f"| {r['id']} | {r['name'].replace('|', '/')} | {r['followers']} | {r['pagerank']:.6f} |"
                      for r in top)
    report = f"""# Résultats de l'analyse (PySpark)

Données : `{data}` — utilisateurs : {n} — liens après nettoyage : {m}.
Composantes connexes : {groups.count()}. PageRank : {iterations} itérations, amortissement 0,85.
Scores PageRank normalisés pour avoir une somme égale à 1.
Exécution : `{spark.sparkContext.master}` ; Spark {spark.version} ; Python {sys.version.split()[0]}.
Calcul des trois métriques (hors lecture et exports) : {seconds:.3f} secondes.

## Les 10 premiers utilisateurs selon PageRank

| ID | Nom | Abonnés | PageRank |
|---|---|---|---|
{table}

Le graphe est fictif. L'influence désigne ici uniquement la centralité PageRank.
Les composantes sont calculées en ignorant le sens des liens : ce ne sont pas
des communautés détectées au sens d'un algorithme de clustering.
Les durées dépendent du matériel et ne sont pas une promesse de performance.
"""
    Path(output).mkdir(parents=True, exist_ok=True)
    Path(output, "rapport.md").write_text(report, encoding="utf-8")
    print(report)


def benchmark(spark, data, output, repetitions, iterations):
    """Chronomètre PageRank + l'action qui force le calcul ; graphe déjà chargé.
    Même format CSV que la version Scala, pour réutiliser scripts/summarize.py."""
    vertices, edges = load(spark, data)
    n, m = vertices.count(), edges.count()

    def once():
        start = time.perf_counter()
        total = pagerank(vertices, edges, iterations).agg(F.sum("rank")).first()[0]
        if not total or total != total:
            raise RuntimeError("PageRank invalide")
        return time.perf_counter() - start

    once()  # Échauffement non inclus dans les mesures.
    times = [once() for _ in range(repetitions)]
    java = spark.sparkContext._jvm.System.getProperty("java.version")
    master = spark.sparkContext.master
    lines = ["master,vertices,edges,iterations,repeat,seconds,spark_version,java_version,partitions"]
    lines += [f'"{master}",{n},{m},{iterations},{i},{t},{spark.version},{java},{edges.rdd.getNumPartitions()}'
              for i, t in enumerate(times, 1)]
    Path(output).mkdir(parents=True, exist_ok=True)
    Path(output, "timings.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Mesures écrites dans {output}/timings.csv")


def main(argv):
    usage = "Usage : social_graph.py analyze DATA OUTPUT [iterations] | benchmark DATA OUTPUT [repetitions] [iterations]"
    if len(argv) < 3 or argv[0] not in ("analyze", "benchmark"):
        sys.exit(usage)
    mode, data, output = argv[:3]
    spark = create_spark()
    try:
        if mode == "analyze":
            iterations = int(argv[3]) if len(argv) > 3 else 20
            if iterations <= 0:
                sys.exit("Le nombre d'itérations doit être positif.")
            analyze(spark, data, output, iterations)
        else:
            repetitions = int(argv[3]) if len(argv) > 3 else 3
            iterations = int(argv[4]) if len(argv) > 4 else 20
            if repetitions <= 0 or iterations <= 0:
                sys.exit("Répétitions et itérations doivent être positives.")
            benchmark(spark, data, output, repetitions, iterations)
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
