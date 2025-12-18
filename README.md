# End-to-End Data Pipeline

## Architecture

Ce projet implémente un pipeline de données moderne suivant l'architecture Medallion (Bronze, Silver, Gold) :

```
API (OpenWeather) → Kafka → S3/MinIO (Bronze) → PySpark (Silver) → PostgreSQL (Gold) → Power BI
                                   ↑
                              Airflow Orchestration
```

### Couches de Données

- **Bronze (Raw)** : Données brutes au format Parquet dans S3/MinIO
- **Silver (Cleaned)** : Données nettoyées et filtrées via PySpark
- **Gold (Aggregated)** : Données structurées dans PostgreSQL prêtes pour l'analyse

## Structure du Projet

```
end-to-end-data-pipeline/
├── src/
│   ├── ingestion/          # Scripts d'extraction de données
│   │   ├── weather_producer.py
│   │   └── kafka_consumer_s3.py
│   ├── processing/         # Transformations PySpark
│   │   └── silver_processor.py
│   └── loading/            # Chargement vers PostgreSQL
│       └── gold_loader.py
├── airflow/
│   └── dags/               # DAGs d'orchestration
│       └── weather_pipeline_dag.py
├── config/                 # Fichiers de configuration
│   └── config.yaml
├── sql/                    # Schémas de base de données
│   └── schema.sql
├── tests/                  # Tests unitaires et d'intégration
│   └── test_pipeline.py
├── docker/                 # Dockerfiles
│   ├── Dockerfile.airflow
│   └── Dockerfile.spark
├── docker-compose.yml      # Orchestration des services
├── requirements.txt        # Dépendances Python
└── README.md
```

## Prérequis

- Docker & Docker Compose
- Python 3.9+
- Compte OpenWeather API (gratuit)

## Installation

1. **Cloner le projet**
```bash
git clone <repo-url>
cd end-to-end-data-pipeline
```

2. **Configurer les variables d'environnement**
```bash
cp .env.example .env
# Éditer .env avec vos clés API
```

3. **Démarrer les services Docker**
```bash
docker-compose up -d
```

4. **Accéder aux interfaces**
- Airflow UI: http://localhost:8080 (admin/admin)
- MinIO Console: http://localhost:9001 (minioadmin/minioadmin)
- Kafka UI: http://localhost:9021

## Utilisation

### 1. Lancer le Pipeline Manuellement

```bash
# Ingestion
python src/ingestion/weather_producer.py

# Processing
python src/processing/silver_processor.py

# Loading
python src/loading/gold_loader.py
```

### 2. Via Airflow

Le DAG `weather_pipeline` s'exécute automatiquement toutes les heures et orchestre :
1. Ingestion des données météo
2. Sauvegarde dans MinIO (Bronze)
3. Transformation PySpark (Silver)
4. Chargement dans PostgreSQL (Gold)

## Tests

```bash
pytest tests/test_pipeline.py -v
```

## Bonnes Pratiques Implémentées

✅ **Modularité** : Séparation claire ETL (Extract, Transform, Load)
✅ **Idempotence** : Utilisation de clés de déduplication
✅ **Logging** : Logs structurés pour chaque étape
✅ **Conteneurisation** : Tous les services dans Docker
✅ **Orchestration** : Gestion des dépendances via Airflow
✅ **Scalabilité** : Architecture distribuée avec Kafka et Spark

## Surveillance et Monitoring

- Logs Airflow : `docker-compose logs airflow-webserver`
- Logs Kafka : `docker-compose logs kafka`
- Métriques PostgreSQL : Via pgAdmin

## Connexion Power BI

1. Ouvrir Power BI Desktop
2. Obtenir des données → PostgreSQL
3. Serveur : `localhost:5432`
4. Base de données : `weather_dw`
5. Sélectionner la table `gold_weather_aggregates`

## Maintenance

### Nettoyage des données anciennes
```sql
DELETE FROM bronze_weather WHERE created_at < NOW() - INTERVAL '30 days';
```

### Redémarrage des services
```bash
docker-compose restart
```

## Licence

MIT
