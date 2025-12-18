# Architecture Détaillée du Pipeline

## Vue d'ensemble

```
┌─────────────────┐
│  OpenWeather    │
│      API        │
└────────┬────────┘
         │ HTTP GET
         ↓
┌─────────────────┐
│  Python Script  │
│  (Producer)     │────→ Kafka Topic: weather-data
└─────────────────┘
         │
         ↓
┌─────────────────┐
│  Kafka Consumer │
│   + MinIO SDK   │────→ S3/MinIO: bronze-layer/
└─────────────────┘       (Parquet files)
         │
         ↓
┌─────────────────┐
│    PySpark      │────→ S3/MinIO: silver-layer/
│  Transformations│       (Cleaned Parquet)
└─────────────────┘
         │
         ↓
┌─────────────────┐
│  Spark JDBC     │────→ PostgreSQL: weather_dw
│   to Postgres   │       (gold_weather_*)
└─────────────────┘
         │
         ↓
┌─────────────────┐
│   Power BI      │
│   Dashboard     │
└─────────────────┘
```

## Couches de Données (Medallion Architecture)

### 🥉 Bronze Layer (Raw Data)
**Emplacement:** MinIO bucket `bronze-layer`  
**Format:** Parquet (compression Snappy)  
**Partitionnement:** `year=YYYY/month=MM/day=DD/`

**Caractéristiques:**
- Données brutes non modifiées depuis Kafka
- Métadonnées Kafka ajoutées (partition, offset, timestamp)
- Idempotent (peut être rejoué sans duplication)
- Rétention: 30 jours

**Exemple de structure:**
```
bronze-layer/
├── year=2024/
│   └── month=01/
│       └── day=15/
│           ├── weather_20240115_120000_123456.parquet
│           └── weather_20240115_130000_789012.parquet
```

### 🥈 Silver Layer (Cleaned Data)
**Emplacement:** MinIO bucket `silver-layer`  
**Format:** Parquet (compression Snappy)  
**Partitionnement:** `year=YYYY/month=MM/`

**Transformations appliquées:**
- ✅ Conversion des types de données (string → timestamp)
- ✅ Filtrage des valeurs nulles critiques
- ✅ Filtrage des températures aberrantes (-100°C à 60°C)
- ✅ Déduplication (city, timestamp)
- ✅ Ajout de colonnes dérivées (year, month, day, hour, etc.)
- ✅ Calcul du score de qualité des données
- ✅ Agrégations journalières et horaires

**Datasets:**
1. `weather_cleaned/` - Données détaillées nettoyées
2. `weather_daily_agg/` - Agrégations journalières
3. `weather_hourly_agg/` - Agrégations horaires

**Rétention:** 90 jours

### 🥇 Gold Layer (Business Data)
**Emplacement:** PostgreSQL database `weather_dw`  
**Format:** Tables relationnelles

**Tables:**

1. **gold_weather_detailed**
   - Données détaillées prêtes pour l'analyse
   - Index sur (city, timestamp)
   - Contrainte unique pour idempotence
   - ~50 colonnes incluant métriques et métadonnées

2. **gold_weather_daily_aggregates**
   - Agrégations quotidiennes par ville
   - Métriques: avg, min, max pour température, humidité, etc.
   - Optimisé pour les tendances à long terme

3. **gold_weather_hourly_aggregates**
   - Agrégations horaires par ville
   - Optimisé pour l'analyse intra-journalière

4. **pipeline_execution_log**
   - Traçabilité de chaque exécution
   - Métriques de performance
   - Gestion des erreurs

**Rétention:** 365 jours

## Flux de Données Détaillé

### 1. Ingestion (weather_producer.py)

```python
# Pseudo-code simplifié
for city in cities:
    raw_data = fetch_from_api(city)
    weather_data = parse_data(raw_data)
    send_to_kafka(weather_data)
```

**Fréquence:** Toutes les heures (configurable)  
**Volume:** ~5 villes × ~2 KB/message = ~10 KB/run  
**Gestion d'erreurs:** Retry automatique (3 tentatives)

### 2. Stockage Bronze (kafka_consumer_s3.py)

```python
# Pseudo-code simplifié
for message in kafka_consumer:
    buffer.append(message)
    
    if len(buffer) >= batch_size:
        df = pd.DataFrame(buffer)
        save_to_s3_as_parquet(df)
        commit_kafka_offset()  # Idempotence
        buffer.clear()
```

**Batch size:** 100 messages  
**Compression:** Gzip (Kafka) + Snappy (Parquet)  
**Idempotence:** Commit Kafka après sauvegarde réussie

### 3. Processing Silver (silver_processor.py)

```python
# Pseudo-code simplifié
df = spark.read.parquet("s3://bronze-layer/")

# Nettoyage
df_clean = df \
    .filter(col("temperature").isNotNull()) \
    .filter(col("temperature").between(-100, 60)) \
    .dropDuplicates(["city", "timestamp"])

# Enrichissement
df_enriched = df_clean \
    .withColumn("year", year("timestamp")) \
    .withColumn("heat_index", calculate_heat_index())

# Agrégations
daily_agg = df_enriched \
    .groupBy("city", "year", "month", "day") \
    .agg(avg("temperature"), ...)

# Sauvegarde
df_enriched.write.parquet("s3://silver-layer/weather_cleaned/")
daily_agg.write.parquet("s3://silver-layer/weather_daily_agg/")
```

**Optimisations Spark:**
- Adaptive Query Execution (AQE)
- Partition coalescing
- Predicate pushdown
- Compression Snappy

### 4. Chargement Gold (gold_loader.py)

```python
# Pseudo-code simplifié
df = spark.read.parquet("s3://silver-layer/weather_cleaned/")

df.write \
    .jdbc(
        url=postgres_url,
        table="gold_weather_detailed",
        mode="append"  # Gestion des doublons par contrainte unique
    )
```

**Mode d'écriture:** Append avec contrainte UNIQUE  
**Gestion des doublons:** PostgreSQL UNIQUE index  
**Batch size:** 1000 enregistrements

## Orchestration Airflow

### DAG: weather_pipeline

**Schedule:** `0 * * * *` (toutes les heures)  
**Max active runs:** 1 (évite les exécutions concurrentes)

**Tâches:**

```
check_api
    ↓
ingest_data
    ↓
consume_to_s3
    ↓
process_silver
    ↓
load_gold
    ↓
quality_check
    ↓
notify_success
    ↓
cleanup_old_data
```

**SLA:** 30 minutes par exécution  
**Retries:** 3 avec délai de 5 minutes  
**Timeout:** 2 heures

## Qualité des Données

### Checks automatiques

1. **Complétude:**
   - Champs obligatoires: city, timestamp, temperature
   - Seuil d'acceptation: 90%

2. **Validité:**
   - Température: -100°C à 60°C
   - Humidité: 0% à 100%
   - Timestamp: dans les 24h

3. **Unicité:**
   - Clé: (city, timestamp)
   - Déduplication automatique

4. **Fraîcheur:**
   - Données < 2 heures recommandé
   - Alerte si > 6 heures

### Score de qualité

Chaque enregistrement reçoit un score (0-100):
- 100: Tous les champs présents
- -10 par champ optionnel manquant

## Performance et Scalabilité

### Capacité actuelle
- **Ingestion:** ~100 messages/minute
- **Processing:** ~10 000 enregistrements/minute
- **Stockage:** ~1 GB/mois (5 villes)

### Scaling horizontal

**Kafka:**
- Augmenter le nombre de partitions
- Ajouter des consumers dans le même groupe

**Spark:**
- Ajouter des workers
- Augmenter la mémoire (executor/driver)

**PostgreSQL:**
- Partitionnement par date
- Read replicas pour Power BI

### Monitoring

**Métriques clés:**
1. Lag Kafka (offset retard)
2. Temps d'exécution Spark
3. Taille des buckets S3
4. Nombre d'erreurs dans pipeline_execution_log

**Alertes:**
- Échec de tâche Airflow → Email
- Lag Kafka > 1000 → Notification
- Espace disque < 20% → Alerte

## Sécurité

### Secrets
- Clés API dans variables d'environnement
- Credentials Kafka/MinIO/PostgreSQL dans `.env`
- Fernet key pour Airflow

### Réseau
- Isolation par Docker network
- Ports exposés uniquement pour interfaces UI

### Données
- Pas de données sensibles (météo publique)
- Logs structurés sans PII

## Maintenance

### Quotidienne
- Vérifier les logs Airflow
- Monitorer les métriques

### Hebdomadaire
- Nettoyer les anciennes données (fonction `cleanup_old_data()`)
- Vérifier l'espace disque

### Mensuelle
- Backup PostgreSQL
- Audit des performances
- Revue des erreurs récurrentes

## Évolutions Futures

### Court terme
- [ ] Alertes Slack/Teams
- [ ] Dashboard Grafana
- [ ] Tests d'intégration CI/CD

### Moyen terme
- [ ] Support multi-APIs (Weather, Finance, etc.)
- [ ] ML pour prédictions météo
- [ ] API REST pour requêter les données

### Long terme
- [ ] Migration vers Kubernetes
- [ ] Data Lake (Delta Lake)
- [ ] Real-time streaming analytics
