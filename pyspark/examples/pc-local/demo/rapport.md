# Résultats de l'analyse (PySpark)

Données : `data/demo` — utilisateurs : 20 — liens après nettoyage : 29.
Composantes connexes : 4. PageRank : 20 itérations, amortissement 0,85.
Scores PageRank normalisés pour avoir une somme égale à 1.
Exécution : `local[2]` ; Spark 3.5.3 ; Python 3.10.12.
Calcul des trois métriques (hors lecture et exports) : 8.640 secondes.

## Les 10 premiers utilisateurs selon PageRank

| ID | Nom | Abonnés | PageRank |
|---|---|---|---|
| 5 | Hicham | 8 | 0.184609 |
| 2 | Sara | 6 | 0.129867 |
| 6 | Lina | 1 | 0.086327 |
| 12 | Leila | 3 | 0.085200 |
| 13 | Rayan | 1 | 0.081973 |
| 11 | Adam | 1 | 0.074929 |
| 18 | Meryem | 2 | 0.068905 |
| 16 | Sofia | 1 | 0.068123 |
| 3 | Youssef | 2 | 0.066320 |
| 17 | Ilyas | 1 | 0.064017 |

Le graphe est fictif. L'influence désigne ici uniquement la centralité PageRank.
Les composantes sont calculées en ignorant le sens des liens : ce ne sont pas
des communautés détectées au sens d'un algorithme de clustering.
Les durées dépendent du matériel et ne sont pas une promesse de performance.
