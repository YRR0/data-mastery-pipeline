"""
Module de chargement des données Silver vers PostgreSQL (couche Gold).
Ce script charge les données transformées dans la base de données relationnelle.
"""

import os
import uuid
from datetime import datetime
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from loguru import logger
from dotenv import load_dotenv
import yaml
import psycopg2
from psycopg2.extras import execute_batch

# Charger les variables d'environnement
load_dotenv()


class PostgreSQLConnection:
    """Gestionnaire de connexion PostgreSQL."""
    
    def __init__(self):
        """Initialise la connexion PostgreSQL."""
        self.conn_params = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', 5432)),
            'database': os.getenv('POSTGRES_DB', 'weather_dw'),
            'user': os.getenv('POSTGRES_USER', 'dataeng'),
            'password': os.getenv('POSTGRES_PASSWORD', 'dataeng123')
        }
        self.conn = None
        self.cursor = None
        logger.info("PostgreSQLConnection initialisé")
    
    def connect(self):
        """Établit la connexion à PostgreSQL."""
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            self.cursor = self.conn.cursor()
            logger.info("Connexion à PostgreSQL établie")
        except psycopg2.Error as e:
            logger.error(f"Erreur de connexion à PostgreSQL: {e}")
            raise
    
    def execute_query(self, query: str, params: tuple = None):
        """
        Exécute une requête SQL.
        
        Args:
            query: Requête SQL
            params: Paramètres de la requête
        """
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            self.conn.commit()
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Erreur lors de l'exécution de la requête: {e}")
            raise
    
    def close(self):
        """Ferme la connexion."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Connexion PostgreSQL fermée")


class PipelineLogger:
    """Logger pour le suivi des exécutions du pipeline."""
    
    def __init__(self, db_conn: PostgreSQLConnection):
        """
        Initialise le logger de pipeline.
        
        Args:
            db_conn: Connexion PostgreSQL
        """
        self.db_conn = db_conn
        self.execution_id = str(uuid.uuid4())
        logger.info(f"PipelineLogger initialisé avec execution_id: {self.execution_id}")
    
    def log_start(self, pipeline_name: str, stage: str):
        """
        Log le début d'une exécution.
        
        Args:
            pipeline_name: Nom du pipeline
            stage: Étape du pipeline
        """
        query = """
        INSERT INTO pipeline_execution_log 
        (execution_id, pipeline_name, stage, status, start_time)
        VALUES (%s, %s, %s, %s, %s)
        """
        self.db_conn.execute_query(
            query,
            (self.execution_id, pipeline_name, stage, 'started', datetime.utcnow())
        )
        logger.info(f"Début de l'exécution: {pipeline_name} - {stage}")
    
    def log_success(
        self, 
        pipeline_name: str, 
        stage: str, 
        records_processed: int,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Log une exécution réussie.
        
        Args:
            pipeline_name: Nom du pipeline
            stage: Étape du pipeline
            records_processed: Nombre d'enregistrements traités
            metadata: Métadonnées additionnelles
        """
        query = """
        UPDATE pipeline_execution_log
        SET status = %s,
            end_time = %s,
            duration_seconds = EXTRACT(EPOCH FROM (end_time - start_time)),
            records_processed = %s,
            metadata = %s::jsonb
        WHERE execution_id = %s AND pipeline_name = %s AND stage = %s
        """
        import json
        metadata_json = json.dumps(metadata) if metadata else None
        
        self.db_conn.execute_query(
            query,
            ('success', datetime.utcnow(), records_processed, metadata_json,
             self.execution_id, pipeline_name, stage)
        )
        logger.info(f"Exécution terminée avec succès: {pipeline_name} - {stage}")
    
    def log_failure(self, pipeline_name: str, stage: str, error_message: str):
        """
        Log une exécution échouée.
        
        Args:
            pipeline_name: Nom du pipeline
            stage: Étape du pipeline
            error_message: Message d'erreur
        """
        query = """
        UPDATE pipeline_execution_log
        SET status = %s,
            end_time = %s,
            duration_seconds = EXTRACT(EPOCH FROM (end_time - start_time)),
            error_message = %s
        WHERE execution_id = %s AND pipeline_name = %s AND stage = %s
        """
        self.db_conn.execute_query(
            query,
            ('failed', datetime.utcnow(), error_message,
             self.execution_id, pipeline_name, stage)
        )
        logger.error(f"Exécution échouée: {pipeline_name} - {stage}")


class GoldLoader:
    """Loader pour la couche Gold (PostgreSQL)."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialise le loader Gold.
        
        Args:
            config_path: Chemin vers le fichier de configuration
        """
        # Charger la configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Créer la session Spark
        self.spark = self._create_spark_session()
        
        # Configuration des chemins
        self.silver_bucket = os.getenv('MINIO_BUCKET_SILVER')
        self.silver_path = f"s3a://{self.silver_bucket}/"
        
        # Connexion PostgreSQL
        self.db_conn = PostgreSQLConnection()
        self.db_conn.connect()
        
        # Logger de pipeline
        self.pipeline_logger = PipelineLogger(self.db_conn)
        
        # Configuration PostgreSQL pour Spark
        self.jdbc_url = (
            f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:"
            f"{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        )
        self.jdbc_properties = {
            "user": os.getenv('POSTGRES_USER'),
            "password": os.getenv('POSTGRES_PASSWORD'),
            "driver": "org.postgresql.Driver"
        }
        
        logger.info("GoldLoader initialisé")
    
    def _create_spark_session(self) -> SparkSession:
        """Crée une session Spark configurée."""
        spark = SparkSession.builder \
            .appName("WeatherGoldLoader") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.5.1") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv('MINIO_ENDPOINT')) \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv('MINIO_ACCESS_KEY')) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv('MINIO_SECRET_KEY')) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        return spark
    
    def read_silver_data(self, dataset_name: str) -> DataFrame:
        """
        Lit les données Silver.
        
        Args:
            dataset_name: Nom du dataset Silver
            
        Returns:
            DataFrame Spark
        """
        try:
            path = f"{self.silver_path}{dataset_name}"
            logger.info(f"Lecture des données Silver depuis: {path}")
            
            df = self.spark.read.parquet(path)
            count = df.count()
            logger.info(f"{count} enregistrements lus depuis Silver/{dataset_name}")
            
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors de la lecture des données Silver: {e}")
            raise
    
    def load_detailed_data(self):
        """Charge les données détaillées dans gold_weather_detailed."""
        stage = "load_detailed"
        pipeline_name = "weather_etl"
        
        try:
            self.pipeline_logger.log_start(pipeline_name, stage)
            
            # Lire les données Silver
            df = self.read_silver_data("weather_cleaned")
            
            # Écrire dans PostgreSQL
            logger.info("Chargement des données détaillées dans PostgreSQL")
            
            df.write \
                .jdbc(
                    url=self.jdbc_url,
                    table="gold_weather_detailed",
                    mode="append",
                    properties=self.jdbc_properties
                )
            
            count = df.count()
            logger.info(f"{count} enregistrements chargés dans gold_weather_detailed")
            
            self.pipeline_logger.log_success(pipeline_name, stage, count)
            
        except Exception as e:
            self.pipeline_logger.log_failure(pipeline_name, stage, str(e))
            raise
    
    def load_daily_aggregates(self):
        """Charge les agrégations journalières."""
        stage = "load_daily_agg"
        pipeline_name = "weather_etl"
        
        try:
            self.pipeline_logger.log_start(pipeline_name, stage)
            
            # Lire les données Silver
            df = self.read_silver_data("weather_daily_agg")
            
            # Écrire dans PostgreSQL avec gestion des doublons (upsert)
            logger.info("Chargement des agrégations journalières dans PostgreSQL")
            
            # Pour PostgreSQL, on utilise INSERT ... ON CONFLICT
            # Via JDBC, on fait un append simple avec la contrainte UNIQUE
            df.write \
                .jdbc(
                    url=self.jdbc_url,
                    table="gold_weather_daily_aggregates",
                    mode="append",
                    properties=self.jdbc_properties
                )
            
            count = df.count()
            logger.info(f"{count} enregistrements chargés dans gold_weather_daily_aggregates")
            
            self.pipeline_logger.log_success(pipeline_name, stage, count)
            
        except Exception as e:
            # En cas de doublon, logger mais ne pas échouer
            if "duplicate key value" in str(e).lower():
                logger.warning(f"Doublons détectés (comportement attendu): {e}")
                self.pipeline_logger.log_success(pipeline_name, stage, 0, 
                                                {"note": "Doublons ignorés"})
            else:
                self.pipeline_logger.log_failure(pipeline_name, stage, str(e))
                raise
    
    def load_hourly_aggregates(self):
        """Charge les agrégations horaires."""
        stage = "load_hourly_agg"
        pipeline_name = "weather_etl"
        
        try:
            self.pipeline_logger.log_start(pipeline_name, stage)
            
            # Lire les données Silver
            df = self.read_silver_data("weather_hourly_agg")
            
            # Écrire dans PostgreSQL
            logger.info("Chargement des agrégations horaires dans PostgreSQL")
            
            df.write \
                .jdbc(
                    url=self.jdbc_url,
                    table="gold_weather_hourly_aggregates",
                    mode="append",
                    properties=self.jdbc_properties
                )
            
            count = df.count()
            logger.info(f"{count} enregistrements chargés dans gold_weather_hourly_aggregates")
            
            self.pipeline_logger.log_success(pipeline_name, stage, count)
            
        except Exception as e:
            if "duplicate key value" in str(e).lower():
                logger.warning(f"Doublons détectés (comportement attendu): {e}")
                self.pipeline_logger.log_success(pipeline_name, stage, 0,
                                                {"note": "Doublons ignorés"})
            else:
                self.pipeline_logger.log_failure(pipeline_name, stage, str(e))
                raise
    
    def load_all(self):
        """Charge toutes les données dans Gold."""
        logger.info("Début du chargement complet vers Gold")
        
        try:
            self.load_detailed_data()
            self.load_daily_aggregates()
            self.load_hourly_aggregates()
            
            logger.info("Chargement complet vers Gold terminé avec succès")
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement vers Gold: {e}")
            raise
    
    def cleanup(self):
        """Nettoie les ressources."""
        logger.info("Nettoyage des ressources")
        self.spark.stop()
        self.db_conn.close()


def main():
    """Point d'entrée principal."""
    logger.add("logs/gold_loading_{time}.log", rotation="1 day", retention="7 days")
    
    try:
        loader = GoldLoader()
        loader.load_all()
        loader.cleanup()
        
    except Exception as e:
        logger.error(f"Erreur fatale dans le loader Gold: {e}")
        raise


if __name__ == "__main__":
    main()
