# TP_BigDATA_M1
# Projet Atelier Big Data

## Description
Ce projet s'inscrit dans le cadre d'un atelier Big Data visant à mettre en place une architecture permettant de collecter, transformer et stocker des données en temps réel. L'objectif principal est de manipuler un flux de transactions financières fictives à l'aide de technologies comme Kafka, Spark et MinIO.

## Technologies utilisées
- **Apache Kafka** : Utilisé comme message broker pour stocker et diffuser les transactions en temps réel.
- **Apache Spark (3.5.3)** : Permet le traitement des flux de données via Spark Streaming.
- **MinIO** : Solution de stockage compatible S3 utilisée pour stocker les données transformées au format Parquet.
- **Docker** : Déploiement des services Kafka et MinIO.
- **Conduktor** : Interface graphique pour la gestion des topics Kafka.
- **Python avec PySpark** : Développement des scripts Producer et Consumer.

## Architecture
Le projet repose sur une architecture orientée streaming :
1. Un **Producer Kafka** envoie des transactions JSON à un topic Kafka.
2. Un **Consumer Spark Streaming** récupère ces données en temps réel.
3. Les transactions sont **transformées** (conversion monétaire, normalisation des dates, nettoyage des erreurs).
4. Les données sont **écrites en Parquet** et stockées dans MinIO.
5. En option, un script permet de **relire les données Parquet** avec Spark pour vérification.

## Étapes d'exécution
### 1️⃣ Mise en place de l'environnement
- Installation de **Docker, Spark, MinIO, Kafka, Conduktor**.
- Configuration des **variables d’environnement** (Hadoop, Java, Spark).

### 2️⃣ Lancement des services
```sh
docker-compose up -d  # Démarre Kafka et MinIO
```

### 3️⃣ Exécution des scripts
- **Producer** : Envoi de transactions vers Kafka
```sh
python producer.py
```
- **Consumer** : Lecture et transformation des transactions
```sh
python consumer.py
```
- **Vérification des données stockées**
```sh
python reader.py
```

## Transformations appliquées
- **Conversion des montants USD → EUR**
- **Ajout d’un fuseau horaire UTC**
- **Formatage des dates en timestamp**
- **Suppression des transactions erronées**
- **Filtrage des données incomplètes**

## Résultats
Les transactions sont stockées dans MinIO sous format **Parquet**, prêtes pour des analyses Big Data avancées.

## Améliorations possibles
- Optimisation du stockage via **partitionnement des fichiers Parquet**.
- Ajout de **tests unitaires** pour le Consumer.
- Intégration de **Kafka Connect** pour relier Kafka à MinIO directement.