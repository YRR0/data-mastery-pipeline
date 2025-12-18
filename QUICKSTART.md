# Guide de démarrage rapide

## 🚀 Démarrage en 5 minutes

### 1. Prérequis
- Docker Desktop installé et en cours d'exécution
- 8 Go de RAM disponible minimum
- 20 Go d'espace disque

### 2. Configuration

Créer le fichier `.env` depuis le template :
```bash
cp .env.example .env
```

Éditer `.env` et ajouter votre clé API OpenWeather :
```bash
OPENWEATHER_API_KEY=votre_cle_api_ici
```

> 💡 Obtenez une clé API gratuite sur https://openweathermap.org/api

### 3. Démarrage

**Linux/Mac:**
```bash
chmod +x start.sh
./start.sh
```

**Windows (PowerShell):**
```powershell
.\start.ps1
```

**Ou manuellement:**
```bash
docker-compose up -d
```

### 4. Accéder aux interfaces

Une fois tous les services démarrés (environ 2 minutes) :

| Service | URL | Identifiants |
|---------|-----|--------------|
| **Airflow** | http://localhost:8080 | admin / admin |
| **MinIO** | http://localhost:9001 | minioadmin / minioadmin |
| **Kafka UI** | http://localhost:9021 | - |
| **Spark** | http://localhost:8081 | - |

### 5. Lancer le pipeline

#### Option A : Via Airflow (Recommandé)

1. Ouvrir Airflow UI : http://localhost:8080
2. Se connecter avec `admin` / `admin`
3. Activer le DAG `weather_pipeline`
4. Cliquer sur "Trigger DAG"

#### Option B : Manuellement

```bash
# 1. Ingestion des données
docker-compose exec airflow-webserver python /opt/airflow/src/ingestion/weather_producer.py

# 2. Consumer Kafka vers S3
docker-compose exec airflow-webserver python /opt/airflow/src/ingestion/kafka_consumer_s3.py

# 3. Processing Silver
docker-compose exec airflow-webserver python /opt/airflow/src/processing/silver_processor.py

# 4. Chargement Gold
docker-compose exec airflow-webserver python /opt/airflow/src/loading/gold_loader.py
```

### 6. Vérifier les données

**PostgreSQL:**
```bash
docker-compose exec postgres psql -U dataeng -d weather_dw -c "SELECT COUNT(*) FROM gold_weather_detailed;"
```

**MinIO:**
Ouvrir http://localhost:9001 et naviguer dans les buckets `bronze-layer` et `silver-layer`

### 7. Visualiser dans Power BI

1. Ouvrir Power BI Desktop
2. Obtenir des données → PostgreSQL
3. Serveur : `localhost:5432`
4. Base de données : `weather_dw`
5. Sélectionner les tables :
   - `gold_weather_detailed`
   - `gold_weather_daily_aggregates`
   - `gold_weather_hourly_aggregates`

## 🔧 Dépannage

### Les services ne démarrent pas

```bash
# Vérifier les logs
docker-compose logs -f

# Redémarrer un service spécifique
docker-compose restart [service_name]
```

### Problème de connexion API

```bash
# Tester manuellement
curl "https://api.openweathermap.org/data/2.5/weather?q=Paris&appid=VOTRE_CLE"
```

### Réinitialiser complètement

```bash
# Arrêter et supprimer tout
docker-compose down -v

# Redémarrer
./start.sh
```

## 📊 Tests

```bash
# Tests unitaires
docker-compose exec airflow-webserver pytest tests/test_pipeline.py -v

# Tests d'intégration
docker-compose exec airflow-webserver python tests/test_integration.py
```

## 🛑 Arrêt

```bash
# Arrêter tous les services
docker-compose down

# Arrêter et supprimer les données
docker-compose down -v
```

## 📝 Configuration avancée

### Changer les villes surveillées

Éditer `config/config.yaml` :
```yaml
ingestion:
  cities:
    - Paris
    - London
    - New York
    - Tokyo
    - Votre Ville
```

### Changer la fréquence d'exécution

Éditer `airflow/dags/weather_pipeline_dag.py` :
```python
schedule_interval='0 * * * *',  # Chaque heure
# Ou
schedule_interval='*/30 * * * *',  # Toutes les 30 minutes
```

## 🆘 Support

Pour toute question ou problème :
1. Vérifier les logs : `docker-compose logs -f [service]`
2. Consulter la documentation complète dans `README.md`
3. Vérifier les issues GitHub

## 🎯 Prochaines étapes

1. Personnaliser les transformations dans `src/processing/silver_processor.py`
2. Ajouter des alertes dans Airflow
3. Créer des dashboards Power BI
4. Implémenter des modèles ML sur les données historiques
