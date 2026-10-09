# Comprendre le projet, étape par étape

## 1. L'idée en une minute

Un réseau social peut être représenté par un graphe orienté. Chaque personne est
un sommet. Une flèche A → B signifie que A suit B. Deux fichiers suffisent : la
liste des personnes et la liste des flèches. Le programme calcule des informations
sur ce réseau, sans créer lui-même un réseau social ou une application de messagerie.

Le jeu de démonstration est fictif. Le nom Hicham fait partie des exemples ; cela
ne signifie pas que les liens décrivent de vraies relations.

## 2. Lire les données

`users.csv` contient `id,name` : identifiant unique et nom affiché.
`follows.csv` contient `src,dst` : utilisateur qui suit et utilisateur suivi.

La ligne `1,2` représente Ahmed qui suit Sara. Elle ajoute un abonnement à Ahmed
et un abonné à Sara. Elle ne signifie pas que Sara suit Ahmed.

Dans la démonstration, les identifiants 1 à 10 forment un ensemble relié, 11 à 15
un deuxième, 16 à 19 un troisième. Le sommet 20 reste seul. On attend donc quatre
composantes, y compris le sommet isolé.

## 3. Compter les abonnés : degré entrant

Le degré entrant compte les flèches qui arrivent sur un sommet. Le degré sortant
compte celles qui en partent. Le code utilise `graph.inDegrees` et `graph.outDegrees`.
Certaines personnes ont un degré nul : une jointure externe conserve leur ligne
et `getOrElse(0)` remplace la valeur absente par zéro.

Sans ces jointures externes, un débutant pourrait perdre les utilisateurs isolés.

## 4. PageRank : comprendre ce qui est calculé

Être suivi par un compte central n'a pas le même poids qu'être suivi par un compte
peu central. PageRank exprime cette idée par une mise à jour répétée des scores.
Un sommet partage son score entre ses voisins sortants.

La récurrence utilisée ici par GraphX, avant normalisation finale, est :

`score_nouveau(v) = 0,15 + 0,85 × somme(score(u) / degré_sortant(u))`

La somme parcourt les utilisateurs u qui suivent v. On part d'un score 1 et on
répète 20 fois par défaut. Enfin, les scores sont divisés par leur somme pour obtenir
des valeurs dont la somme est 1. GraphX possède sa propre normalisation globale ;
la division finale de l'application fixe explicitement notre convention de sortie.

**Convention à connaître :** dans cette variante non personnalisée de GraphX,
un sommet sans sortie n'envoie pas de contribution à l'étape suivante. La récurrence
ne redistribue pas à chaque tour la masse des puits comme certaines formulations
probabilistes du PageRank. Notre test de référence suit la convention GraphX.
Il ne faut pas comparer aveuglément ces chiffres à une autre bibliothèque utilisant
une convention ou un critère d'arrêt différent.

Le score mesure une centralité structurelle. Il ne mesure pas la qualité d'une personne,
ses compétences, sa popularité réelle ni une probabilité de vente.

## 5. Composantes connexes

Deux personnes appartiennent à la même composante s'il existe un chemin entre elles
après avoir ignoré le sens des liens. Ce n'est pas la même chose qu'être directement
abonnés l'un à l'autre, ni que pouvoir se rejoindre en respectant toutes les flèches.

GraphX étiquette chaque composante avec son plus petit identifiant. L'identifiant
sert de label : il ne désigne pas le membre le plus influent. La méthode ne constitue
pas une détection fine de communautés.

## 6. Spark et GraphX

- **Spark** répartit les données en partitions et exécute des tâches sur ces partitions.
- Un **RDD** est une collection partitionnée pouvant être recalculée à partir de son historique.
- **GraphX** représente les sommets et les arêtes à l'aide de structures distribuées.
- Le **driver** organise l'application ; les **executors** effectuent les tâches.
- En mode `local[2]`, le calcul s'effectue dans le processus local avec deux threads de tâches.

Le programme est écrit avec les API distribuées, mais la démonstration fonctionne
sur une seule machine. Ne pas la présenter comme un déploiement de production
sur plusieurs serveurs. Docker fournit un environnement ; il n'ajoute ni CPU physique
ni GPU à votre ordinateur.

## 7. Lecture du code Scala

Ouvrir `src/main/scala/SocialGraph.scala` dans cet ordre :

1. `load` : schémas CSV, vérifications, suppression des liens répétés et création du graphe.
2. `metrics` : degrés, PageRank, composantes et jointures par identifiant.
3. Branche `analyze` de `main` : calcul effectif, exports et rapport.
4. Branche `benchmark` : échauffement, répétitions et chronométrage.
5. `GraphChecks` : petites données et réponses attendues indépendantes.

`map` transforme chaque élément. `filter` sélectionne des éléments. `join` rapproche
deux collections par clé. `case` décompose les tuples renvoyés par les jointures.
`Option` représente une valeur potentiellement absente.

Spark évalue beaucoup d'opérations de manière paresseuse : définir un résultat ne
l'exécute pas forcément. Une action comme `count`, `sum` ou l'écriture déclenche
le calcul. C'est pourquoi le chronomètre inclut une action.

`cache` garde les données réutilisées en mémoire si possible. `unpersist` libère
les caches. Le benchmark libère les caches créés par PageRank entre répétitions,
tout en conservant le graphe d'entrée. Des effets de JVM et du système demeurent.

## 8. Comprendre les mesures

Notons T1 la durée médiane avec un cœur et Tp avec p cœurs.

- Accélération : `S(p) = T1 / Tp`.
- Efficacité : `E(p) = S(p) / p`.

Si T1 = 10 s et T2 = 6 s, l'accélération vaut 1,67 et l'efficacité environ 83 %.
Ces nombres sont un **exemple pédagogique**, pas des résultats du projet.

Le script produit trois durées par configuration après un échauffement et retient
la médiane. Il rapporte aussi minimum et maximum. Pour une étude plus sérieuse,
augmenter les répétitions, changer l'ordre des configurations entre campagnes
et mesurer plusieurs tailles de graphe.

L'accélération peut être inférieure à 1 : coordination des tâches, échanges de données,
garbage collector, petites partitions et faible volume peuvent annuler le gain.
Le graphe est réparti en autant de partitions d'arêtes que de cœurs Spark
(`local[N]` → N partitions) : parallélisme et partitionnement varient ensemble ;
noter cette limite dans toute comparaison.

## 9. Pourquoi ces tests ?

Un programme qui termine sans erreur peut donner un résultat faux. Les tests vérifient :

- les degrés sur un graphe connu ;
- la présence d'un sommet isolé ;
- les composantes attendues ;
- PageRank comparé à une récurrence séquentielle indépendante ;
- les scores égaux d'un cycle symétrique ;
- la suppression des doublons et des boucles ;
- le rejet d'un lien vers une personne inconnue.

Les tests ne démontrent pas l'absence de tous les bugs. Ils ciblent les erreurs
importantes pour les fonctionnalités livrées.

## 10. Ce que tu dois pouvoir expliquer en entretien

Avant de présenter le projet comme une compétence maîtrisée, entraîne-toi à répondre :

1. Que représentent un sommet et une arête dans tes fichiers ?
2. Pourquoi PageRank ne se limite-t-il pas au nombre d'abonnés ?
3. Pourquoi conserver un sommet isolé ?
4. Que signifie exactement une composante connexe ?
5. Quelle opération déclenche réellement le calcul Spark ?
6. Que mesure ton chronomètre et qu'exclut-il ?
7. Pourquoi quatre cœurs ne sont-ils pas forcément quatre fois plus rapides ?
8. Qu'as-tu testé localement et que faudrait-il adapter sur un vrai cluster ?

Une fois le code exécuté et compris, ta présentation doit décrire tes mesures réelles,
tes adaptations et les limites que tu as constatées. Les algorithmes PageRank et
composantes connexes viennent de GraphX : ne prétends pas les avoir inventés ou
réimplémentés dans ce projet.

## 11. Exercices pour prendre le projet en main

1. Ajouter le lien `20,5`. Prédire le nouveau nombre de composantes, puis vérifier : trois.
2. Ajouter deux fois `20,5`. Vérifier que le nombre d'abonnés de 5 n'augmente que d'un.
3. Essayer le lien `20,99`. Observer le rejet de l'identifiant inconnu.
4. Comparer PageRank à 5, 20 et 50 itérations, sans supposer la convergence.
5. Générer 1 000 puis 10 000 utilisateurs et comparer les coûts de calcul.

Garder une copie des CSV d'origine pour restaurer la démonstration.

## 12. Dépannage

- `docker: permission denied` : votre utilisateur n'a pas accès au moteur Docker ;
  régler cet accès avant de relancer. Ne pas rendre la socket accessible à tous.
- `docker compose` inconnu : le plugin Compose v2 manque.
- Échec de téléchargement : vérifier la connexion et l'accès au registre Docker.
- Mémoire insuffisante : fermer les applications lourdes, réduire les données ;
  augmenter `DRIVER_MEMORY` uniquement si la RAM disponible le permet.
- Permission sur `results/` : utiliser `demo.sh` ou définir `LOCAL_UID` et `LOCAL_GID`.
- Le premier lancement est lent : construction de l'image et démarrage JVM.
- Plusieurs fichiers CSV : c'est le comportement normal des exports Spark.
