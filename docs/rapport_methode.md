# Rapport sur la méthode de détection

## Contexte

L'objectif est de détecter un événement social anormal dans un flux de posts simulés, sans utiliser Twitter réel. Le pipeline retenu suit la chaîne imposée par l'énoncé: producteur Python, Kafka, Spark et stockage des anomalies dans PostgreSQL.

## Génération du flux

Le producteur émet des posts synthétiques dans trois langues: français, anglais et arabe. En phase normale, les messages sont variés et la présence du mot clé cible reste faible. Pendant une fenêtre contrôlée, le producteur injecte un pic artificiel de posts en français contenant le mot clé `manifestation`. Un bruit secondaire dans une autre langue peut être produit en parallèle afin de vérifier que le filtrage linguistique reste correct.

## Méthode de détection

La détection repose sur quatre étapes:

1. lecture continue du topic Kafka avec Spark Structured Streaming;
2. parsing JSON et conversion du champ temporel en timestamp;
3. filtrage des messages de langue française contenant le mot clé cible;
4. agrégation par fenêtres tumbling de 15 secondes.

Chaque fenêtre produit un nombre d'occurrences. Ce nombre est comparé à une baseline dynamique calculée à partir des six fenêtres précédentes du même couple `(mot clé, langue)`.

- moyenne dynamique: moyenne des comptes historiques;
- dispersion: écart-type des comptes historiques;
- seuil retenu: `max(mean + 3*stddev, mean*2, 10)`.

Le seuil plancher à `10` évite de marquer comme anormal un bruit faible pendant la phase d'amorçage. L'algorithme n'autorise la détection qu'après au moins quatre fenêtres d'historique afin d'obtenir une baseline minimale.

## Justification

Cette approche est adaptée à l'énoncé pour trois raisons:

- elle respecte explicitement la contrainte de fenêtres tumbling;
- la baseline est dynamique et s'ajuste au niveau de trafic observé;
- le filtrage par langue réduit les faux positifs créés par un mot proche ou un bruit dans une autre langue.

## Limites

Le flux est synthétique et le comportement des utilisateurs est simplifié. Dans un contexte réel, il faudrait enrichir la détection avec plusieurs mots clés, une normalisation linguistique plus robuste et une gestion des retards d'événements plus poussée.
