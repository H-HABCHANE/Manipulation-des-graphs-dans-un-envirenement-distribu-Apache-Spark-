# Comparaison PageRank

10000 sommets, 80000 liens, 20 itérations.
Spark 3.5.3 ; Java 17.0.12. Un échauffement avant chaque série.

| Cœurs Spark | Médiane (s) | Min (s) | Max (s) | Répétitions | Accélération | Efficacité |
|---|---|---|---|---|---|---|
| 1 | 2.546 | 2.475 | 2.577 | 3 | 1.00 | 100.0% |
| 2 | 3.117 | 2.708 | 3.183 | 3 | 0.82 | 40.8% |
| 4 | 2.883 | 2.498 | 3.292 | 3 | 0.88 | 22.1% |

Accélération = médiane à 1 cœur / médiane à p cœurs.
Efficacité = accélération / p. Les cœurs désignent les threads de calcul autorisés à Spark.
Mesures locales sur un seul hôte, graphe déjà chargé. Le démarrage JVM et les exports sont exclus.
Ce protocole fait varier le parallélisme et le partitionnement par défaut de Spark ; ce n'est pas un test à partitionnement fixe.
Le coût de coordination peut dépasser le gain sur de petits graphes. Une accélération n'est pas garantie.
Pour comparer deux machines, conserver le même jeu de données et documenter CPU, RAM, système et charge.

![Durées mesurées](timings.svg)
