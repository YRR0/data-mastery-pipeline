-- =====================================================
-- Schéma de base de données PostgreSQL pour la couche Gold
-- Pipeline de données météorologiques
-- =====================================================

-- Création du schéma si nécessaire
CREATE SCHEMA IF NOT EXISTS public;

-- =====================================================
-- Table: gold_weather_detailed
-- Description: Données météo nettoyées et enrichies au niveau détaillé
-- =====================================================
DROP TABLE IF EXISTS gold_weather_detailed CASCADE;

CREATE TABLE gold_weather_detailed (
    -- Identifiant unique
    id BIGSERIAL PRIMARY KEY,
    
    -- Clés métier
    city VARCHAR(100) NOT NULL,
    country VARCHAR(10) NOT NULL,
    
    -- Timestamps
    timestamp TIMESTAMP NOT NULL,
    ingestion_timestamp TIMESTAMP NOT NULL,
    processing_timestamp TIMESTAMP NOT NULL,
    
    -- Mesures météorologiques
    temperature NUMERIC(5, 2),
    feels_like NUMERIC(5, 2),
    temp_min NUMERIC(5, 2),
    temp_max NUMERIC(5, 2),
    temp_range NUMERIC(5, 2),
    pressure INTEGER,
    humidity INTEGER,
    visibility INTEGER,
    
    -- Vent
    wind_speed NUMERIC(6, 2),
    wind_deg INTEGER,
    
    -- Nuages et conditions
    clouds INTEGER,
    weather_main VARCHAR(50),
    weather_description VARCHAR(100),
    
    -- Soleil
    sunrise TIMESTAMP,
    sunset TIMESTAMP,
    is_day BOOLEAN,
    
    -- Colonnes dérivées
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    hour INTEGER NOT NULL,
    day_of_week INTEGER,
    week_of_year INTEGER,
    quarter INTEGER,
    is_weekend BOOLEAN,
    heat_index NUMERIC(5, 2),
    
    -- Qualité des données
    data_quality_score INTEGER,
    
    -- Métadonnées Kafka (pour traçabilité)
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP,
    
    -- Timestamp de chargement
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Contraintes
    CONSTRAINT chk_temperature CHECK (temperature BETWEEN -100 AND 60),
    CONSTRAINT chk_humidity CHECK (humidity BETWEEN 0 AND 100),
    CONSTRAINT chk_quality_score CHECK (data_quality_score BETWEEN 0 AND 100)
);

-- Index pour les requêtes fréquentes
CREATE INDEX idx_weather_detailed_city_timestamp ON gold_weather_detailed(city, timestamp DESC);
CREATE INDEX idx_weather_detailed_timestamp ON gold_weather_detailed(timestamp DESC);
CREATE INDEX idx_weather_detailed_city_date ON gold_weather_detailed(city, year, month, day);

-- Contrainte d'unicité pour éviter les doublons (idempotence)
CREATE UNIQUE INDEX idx_weather_detailed_unique ON gold_weather_detailed(city, timestamp);

-- =====================================================
-- Table: gold_weather_daily_aggregates
-- Description: Agrégations journalières par ville
-- =====================================================
DROP TABLE IF EXISTS gold_weather_daily_aggregates CASCADE;

CREATE TABLE gold_weather_daily_aggregates (
    -- Identifiant unique
    id BIGSERIAL PRIMARY KEY,
    
    -- Clés métier
    city VARCHAR(100) NOT NULL,
    country VARCHAR(10) NOT NULL,
    
    -- Période
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    date DATE GENERATED ALWAYS AS (make_date(year, month, day)) STORED,
    
    -- Métriques agrégées
    avg_temperature NUMERIC(5, 2),
    min_temperature NUMERIC(5, 2),
    max_temperature NUMERIC(5, 2),
    avg_humidity NUMERIC(5, 2),
    avg_wind_speed NUMERIC(6, 2),
    avg_pressure NUMERIC(7, 2),
    
    -- Compteurs
    record_count INTEGER NOT NULL,
    
    -- Métadonnées
    aggregation_level VARCHAR(20) DEFAULT 'daily',
    last_updated TIMESTAMP NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Contrainte d'unicité
    CONSTRAINT uq_daily_agg UNIQUE (city, year, month, day)
);

-- Index pour les requêtes fréquentes
CREATE INDEX idx_daily_agg_city_date ON gold_weather_daily_aggregates(city, date DESC);
CREATE INDEX idx_daily_agg_date ON gold_weather_daily_aggregates(date DESC);

-- =====================================================
-- Table: gold_weather_hourly_aggregates
-- Description: Agrégations horaires par ville
-- =====================================================
DROP TABLE IF EXISTS gold_weather_hourly_aggregates CASCADE;

CREATE TABLE gold_weather_hourly_aggregates (
    -- Identifiant unique
    id BIGSERIAL PRIMARY KEY,
    
    -- Clés métier
    city VARCHAR(100) NOT NULL,
    country VARCHAR(10) NOT NULL,
    
    -- Période
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    hour INTEGER NOT NULL,
    datetime TIMESTAMP GENERATED ALWAYS AS (
        make_timestamp(year, month, day, hour, 0, 0)
    ) STORED,
    
    -- Métriques agrégées
    avg_temperature NUMERIC(5, 2),
    min_temperature NUMERIC(5, 2),
    max_temperature NUMERIC(5, 2),
    avg_humidity NUMERIC(5, 2),
    avg_wind_speed NUMERIC(6, 2),
    avg_pressure NUMERIC(7, 2),
    
    -- Compteurs
    record_count INTEGER NOT NULL,
    
    -- Métadonnées
    aggregation_level VARCHAR(20) DEFAULT 'hourly',
    last_updated TIMESTAMP NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Contrainte d'unicité
    CONSTRAINT uq_hourly_agg UNIQUE (city, year, month, day, hour)
);

-- Index pour les requêtes fréquentes
CREATE INDEX idx_hourly_agg_city_datetime ON gold_weather_hourly_aggregates(city, datetime DESC);
CREATE INDEX idx_hourly_agg_datetime ON gold_weather_hourly_aggregates(datetime DESC);

-- =====================================================
-- Table: pipeline_execution_log
-- Description: Log d'exécution du pipeline pour monitoring
-- =====================================================
DROP TABLE IF EXISTS pipeline_execution_log CASCADE;

CREATE TABLE pipeline_execution_log (
    id BIGSERIAL PRIMARY KEY,
    execution_id UUID DEFAULT gen_random_uuid(),
    pipeline_name VARCHAR(100) NOT NULL,
    stage VARCHAR(50) NOT NULL,  -- ingestion, processing, loading
    status VARCHAR(20) NOT NULL,  -- started, success, failed
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    records_processed INTEGER,
    records_failed INTEGER,
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour les requêtes de monitoring
CREATE INDEX idx_pipeline_log_execution_id ON pipeline_execution_log(execution_id);
CREATE INDEX idx_pipeline_log_pipeline_name ON pipeline_execution_log(pipeline_name, start_time DESC);
CREATE INDEX idx_pipeline_log_status ON pipeline_execution_log(status);

-- =====================================================
-- Vues pour Power BI
-- =====================================================

-- Vue: Dernières mesures par ville
CREATE OR REPLACE VIEW v_latest_weather AS
SELECT DISTINCT ON (city)
    city,
    country,
    timestamp,
    temperature,
    feels_like,
    humidity,
    wind_speed,
    weather_main,
    weather_description
FROM gold_weather_detailed
ORDER BY city, timestamp DESC;

-- Vue: Statistiques hebdomadaires
CREATE OR REPLACE VIEW v_weekly_stats AS
SELECT
    city,
    country,
    year,
    week_of_year,
    AVG(avg_temperature) AS avg_temp,
    MIN(min_temperature) AS min_temp,
    MAX(max_temperature) AS max_temp,
    AVG(avg_humidity) AS avg_humidity,
    SUM(record_count) AS total_records
FROM gold_weather_daily_aggregates
GROUP BY city, country, year, week_of_year
ORDER BY year DESC, week_of_year DESC;

-- Vue: Comparaison jour/nuit
CREATE OR REPLACE VIEW v_day_night_comparison AS
SELECT
    city,
    DATE(timestamp) AS date,
    is_day,
    AVG(temperature) AS avg_temperature,
    AVG(humidity) AS avg_humidity,
    COUNT(*) AS measurement_count
FROM gold_weather_detailed
GROUP BY city, DATE(timestamp), is_day
ORDER BY date DESC;

-- =====================================================
-- Fonctions utilitaires
-- =====================================================

-- Fonction: Nettoyer les anciennes données
CREATE OR REPLACE FUNCTION cleanup_old_data(retention_days INTEGER DEFAULT 30)
RETURNS TABLE(table_name VARCHAR, deleted_count BIGINT) AS $$
BEGIN
    -- Nettoyer gold_weather_detailed
    DELETE FROM gold_weather_detailed
    WHERE timestamp < CURRENT_TIMESTAMP - (retention_days || ' days')::INTERVAL;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    table_name := 'gold_weather_detailed';
    RETURN NEXT;
    
    -- Nettoyer gold_weather_daily_aggregates
    DELETE FROM gold_weather_daily_aggregates
    WHERE date < CURRENT_DATE - retention_days;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    table_name := 'gold_weather_daily_aggregates';
    RETURN NEXT;
    
    -- Nettoyer gold_weather_hourly_aggregates
    DELETE FROM gold_weather_hourly_aggregates
    WHERE datetime < CURRENT_TIMESTAMP - (retention_days || ' days')::INTERVAL;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    table_name := 'gold_weather_hourly_aggregates';
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- Fonction: Statistiques de la base
CREATE OR REPLACE FUNCTION get_database_stats()
RETURNS TABLE(
    metric_name VARCHAR,
    metric_value BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 'total_detailed_records'::VARCHAR, COUNT(*)::BIGINT
    FROM gold_weather_detailed
    UNION ALL
    SELECT 'total_daily_aggregates'::VARCHAR, COUNT(*)::BIGINT
    FROM gold_weather_daily_aggregates
    UNION ALL
    SELECT 'total_hourly_aggregates'::VARCHAR, COUNT(*)::BIGINT
    FROM gold_weather_hourly_aggregates
    UNION ALL
    SELECT 'distinct_cities'::VARCHAR, COUNT(DISTINCT city)::BIGINT
    FROM gold_weather_detailed
    UNION ALL
    SELECT 'pipeline_executions'::VARCHAR, COUNT(*)::BIGINT
    FROM pipeline_execution_log;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- Grants (à adapter selon vos besoins)
-- =====================================================
-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO dataeng;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO dataeng;

-- =====================================================
-- Commentaires sur les tables
-- =====================================================
COMMENT ON TABLE gold_weather_detailed IS 'Données météo détaillées nettoyées et enrichies (couche Gold)';
COMMENT ON TABLE gold_weather_daily_aggregates IS 'Agrégations journalières des données météo par ville';
COMMENT ON TABLE gold_weather_hourly_aggregates IS 'Agrégations horaires des données météo par ville';
COMMENT ON TABLE pipeline_execution_log IS 'Journal d''exécution du pipeline pour monitoring';

-- =====================================================
-- Fin du script
-- =====================================================
