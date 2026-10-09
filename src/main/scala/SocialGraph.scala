import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel

/** Analyse de graphes orientés : un lien source -> destination signifie « suit ». */
object SocialGraph {
  private def saveText(path: String, text: String): Unit = {
    val p = Paths.get(path)
    Option(p.getParent).foreach(Files.createDirectories(_))
    Files.write(p, text.getBytes(StandardCharsets.UTF_8))
  }

  def load(spark: SparkSession, directory: String): Graph[String, Int] = {
    val usersSchema = new StructType().add("id", LongType).add("name", StringType)
    val linksSchema = new StructType().add("src", LongType).add("dst", LongType)
    def csv(file: String, schema: StructType): DataFrame = spark.read
      .option("header", "true").option("mode", "FAILFAST")
      .option("enforceSchema", "false").schema(schema).csv(s"$directory/$file")
    val users = csv("users.csv", usersSchema).cache()
    val links = csv("follows.csv", linksSchema).cache()
    try {
      require(users.count() > 0, "Le fichier users.csv doit contenir au moins un utilisateur.")
      require(users.filter("id IS NULL OR name IS NULL OR trim(name) = ''").count() == 0,
        "Chaque utilisateur doit avoir un identifiant entier et un nom non vide.")
      require(users.groupBy("id").count().filter("count > 1").count() == 0,
        "Identifiants utilisateurs dupliqués.")
      require(links.filter("src IS NULL OR dst IS NULL").count() == 0,
        "Chaque lien doit avoir deux identifiants entiers.")
      val ids = users.selectExpr("id AS vertex")
      val endpoints = links.selectExpr("src AS vertex").union(links.selectExpr("dst AS vertex"))
      require(endpoints.join(ids, Seq("vertex"), "left_anti").count() == 0,
        "Un lien référence un utilisateur absent de users.csv.")
      // Un abonnement à soi-même est ignoré ; un abonnement répété ne compte qu'une fois.
      val cleanLinks = links.filter("src <> dst").distinct()
      val vertices = users.rdd.map(r => (r.getLong(0), r.getString(1)))
      val edges = cleanLinks.rdd.map(r => Edge(r.getLong(0), r.getLong(1), 1))
      // Un petit CSV est lu en une seule partition : répartir les liens sur les cœurs disponibles.
      val partitions = spark.sparkContext.defaultParallelism
      val graph = Graph(vertices, edges, "inconnu")
        .partitionBy(PartitionStrategy.EdgePartition2D, partitions).cache()
      graph.vertices.count(); graph.edges.count()
      graph
    } finally { users.unpersist(); links.unpersist() }
  }

  /** Jointures externes : elles conservent les utilisateurs sans aucun lien. */
  def metrics(graph: Graph[String, Int], iterations: Int): org.apache.spark.rdd.RDD[(Long, String, Int, Int, Double, Long)] = {
    val ranks = graph.staticPageRank(iterations).vertices
    val total = ranks.values.sum()
    val components = graph.connectedComponents().vertices
    graph.vertices.leftOuterJoin(graph.inDegrees).leftOuterJoin(graph.outDegrees)
      .join(ranks).join(components).map {
        case (id, ((((name, in), out), rank), component)) =>
          (id, name, in.getOrElse(0), out.getOrElse(0), rank / total, component)
      }
  }

  def main(args: Array[String]): Unit = {
    require(args.length >= 1, "Usage : SocialGraph test | analyze DATA OUTPUT [iterations] | benchmark DATA OUTPUT [repetitions] [iterations]")
    val spark = SparkSession.builder().appName("Spark Social Graph").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    try {
      if (args(0) == "test") { GraphChecks.run(spark); return }
      require(Set("analyze", "benchmark").contains(args(0)) && args.length >= 3, "Commande ou arguments invalides.")
      val mode = args(0); val input = args(1); val output = args(2)
      val iterations = if (mode == "analyze") args.lift(3).map(_.toInt).getOrElse(20)
        else args.lift(4).map(_.toInt).getOrElse(20)
      require(iterations > 0, "Le nombre d'itérations doit être positif.")
      val graph = load(spark, input)
      val n = graph.vertices.count(); val m = graph.edges.count()
      if (mode == "analyze") {
        val start = System.nanoTime()
        val rows = metrics(graph, iterations).persist(StorageLevel.MEMORY_AND_DISK)
        rows.count() // Spark est paresseux : déclencher réellement tous les calculs.
        val seconds = (System.nanoTime() - start) / 1e9
        val df = rows.toDF("id", "name", "followers", "following", "pagerank", "component")
        df.write.mode("overwrite").option("header", "true").csv(s"$output/users")
        val groups = df.groupBy("component").count().orderBy($"count".desc, $"component")
        groups.write.mode("overwrite").option("header", "true").csv(s"$output/components")
        val top = df.orderBy($"pagerank".desc, $"id").limit(10).collect()
        val table = top.map(r => f"| ${r.getLong(0)} | ${r.getString(1).replace("|", "/")} | ${r.getInt(2)} | ${r.getDouble(4)}%.6f |").mkString("\n")
        val report = s"""# Résultats de l'analyse
|
|Données : `$input` — utilisateurs : $n — liens après nettoyage : $m.
|Composantes connexes : ${groups.count()}. PageRank : $iterations itérations, amortissement 0,85.
|Scores PageRank normalisés pour avoir une somme égale à 1.
|Exécution : `${spark.sparkContext.master}` ; Spark ${spark.version}.
|Calcul des trois métriques (hors lecture et exports) : $seconds secondes.
|
|## Les 10 premiers utilisateurs selon PageRank
|
|| ID | Nom | Abonnés | PageRank |
||---|---|---|---|
|__TABLE__
|
|Le graphe est fictif. L'influence désigne ici uniquement la centralité PageRank.
|Les composantes sont calculées en ignorant le sens des liens : ce ne sont pas
|des communautés détectées au sens d'un algorithme de clustering.
|Les durées dépendent du matériel et ne sont pas une promesse de performance.
|""".stripMargin.replace("__TABLE__", table)
        saveText(s"$output/rapport.md", report)
        println(report)
        rows.unpersist()
      } else {
        val repetitions = args.lift(3).map(_.toInt).getOrElse(3)
        require(repetitions > 0, "Le nombre de répétitions doit être positif.")
        def once(): Double = {
          val before = spark.sparkContext.getPersistentRDDs.keySet
          val start = System.nanoTime()
          val ranks = graph.staticPageRank(iterations).vertices
          val sum = ranks.values.sum() // action : toutes les partitions sont évaluées.
          require(sum > 0 && !sum.isNaN, "PageRank invalide")
          val seconds = (System.nanoTime() - start) / 1e9
          // Libérer uniquement les caches de l'algorithme ; conserver le graphe d'entrée.
          spark.sparkContext.getPersistentRDDs.filterNot { case (id, _) => before(id) }
            .values.foreach(_.unpersist(blocking = true))
          seconds
        }
        once() // Échauffement non inclus dans les mesures.
        val times = (1 to repetitions).map(i => (i, once()))
        val master = spark.sparkContext.master
        val header = "master,vertices,edges,iterations,repeat,seconds,spark_version,java_version,partitions\n"
        val csv = times.map { case (i, t) =>
          s""""$master",$n,$m,$iterations,$i,$t,${spark.version},${System.getProperty("java.version")},${graph.edges.getNumPartitions}"""
        }.mkString("\n")
        saveText(s"$output/timings.csv", header + csv + "\n")
        println(s"Mesures écrites dans $output/timings.csv")
      }
      graph.unpersist()
    } finally { spark.stop() }
  }
}

/** Vérifications indépendantes sur des graphes où l'on connaît la réponse. */
object GraphChecks {
  def run(spark: SparkSession): Unit = {
    val sc = spark.sparkContext
    val vertices = sc.parallelize(Seq((1L,"A"),(2L,"B"),(3L,"C"),(4L,"Isolé")))
    val links = Seq((1L,2L),(2L,1L),(2L,3L))
    val graph = Graph(vertices, sc.parallelize(links.map { case (s,d) => Edge(s,d,1) }))
    val result = SocialGraph.metrics(graph, 20).collect().map(x => x._1 -> x).toMap
    assert(result(1)._3 == 1 && result(2)._3 == 1 && result(3)._3 == 1 && result(4)._3 == 0)
    assert(result(1)._4 == 1 && result(2)._4 == 2 && result(3)._4 == 0 && result(4)._4 == 0)
    assert(result(1)._6 == result(2)._6 && result(2)._6 == result(3)._6)
    assert(result(4)._6 != result(1)._6)
    // Référence séquentielle de la récurrence GraphX (sans redistribution des puits).
    var ranks = (1L to 4L).map(_ -> 1.0).toMap
    val degrees = links.groupBy(_._1).map { case (id, e) => id -> e.size }
    for (_ <- 1 to 20) {
      ranks = (1L to 4L).map { id =>
        id -> (0.15 + 0.85 * links.filter(_._2 == id).map { case (s,_) => ranks(s) / degrees(s) }.sum)
      }.toMap
    }
    val total = ranks.values.sum
    for (id <- 1L to 4L) assert(math.abs(result(id)._5 - ranks(id) / total) < 1e-8, s"PageRank incorrect : $id")
    assert(math.abs(result.values.map(_._5).sum - 1.0) < 1e-10)
    // Cycle symétrique : les trois scores doivent être égaux.
    val cycle = Graph(vertices.filter(_._1 != 4), sc.parallelize(Seq(Edge(1L,2L,1),Edge(2L,3L,1),Edge(3L,1L,1))))
    SocialGraph.metrics(cycle, 10).collect().foreach(r => assert(math.abs(r._5 - 1.0/3) < 1e-10))
    val temporary = Files.createTempDirectory("graph-checks-")
    def put(name: String, text: String): Unit = Files.write(temporary.resolve(name), text.getBytes(StandardCharsets.UTF_8))
    put("users.csv", "id,name\n1,A\n2,B\n3,Isolé\n")
    put("follows.csv", "src,dst\n1,2\n1,2\n1,1\n")
    val loaded = SocialGraph.load(spark, temporary.toString)
    assert(loaded.vertices.count() == 3 && loaded.edges.count() == 1)
    loaded.unpersist()
    put("follows.csv", "src,dst\n1,99\n")
    var rejected = false
    try { SocialGraph.load(spark, temporary.toString) } catch { case _: IllegalArgumentException => rejected = true }
    assert(rejected, "Les références inconnues doivent être rejetées")
    Files.delete(temporary.resolve("users.csv")); Files.delete(temporary.resolve("follows.csv")); Files.delete(temporary)
    println("TESTS OK : degrés, composantes, sommet isolé, PageRank de référence, cycle, nettoyage et validation.")
  }
}
