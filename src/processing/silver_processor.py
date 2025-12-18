"""
Module de processing des données Bronze vers Silver avec PySpark.
Ce script nettoie, filtre et agrège les données météo brutes.
"""

import os
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType
)
from pyspark.sql.window import Window
from loguru import logger
from dotenv import load_dotenv
import yaml

# Charger les variables d'environnement
load_dotenv()


class SparkConfig:
    """Configuration pour Spark."""
    
    @staticmethod
    def create_spark_session(app_name: str = "WeatherSilverProcessor") -> SparkSession:
        """
        Crée une session Spark avec la configuration optimale.
        
        Args:
            app_name: Nom de l'application Spark
            
        Returns:
            SparkSession configurée
        """
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv('MINIO_ENDPOINT', 'localhost:9000')) \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv('MINIO_ACCESS_KEY', 'minioadmin')) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv('MINIO_SECRET_KEY', 'minioadmin')) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        # Réduire le niveau de log
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"SparkSession créée: {app_name}")
        return spark


class WeatherDataSchema:
    """Schéma des données météo."""
    
    @staticmethod
    def get_bronze_schema() -> StructType:
        """Retourne le schéma des données Bronze."""
        return StructType([
            StructField("city", StringType(), False),
            StructField("country", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("temperature", DoubleType(), True),
            StructField("feels_like", DoubleType(), True),
            StructField("temp_min", DoubleType(), True),
            StructField("temp_max", DoubleType(), True),
            StructField("pressure", IntegerType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("visibility", IntegerType(), True),
            StructField("wind_speed", DoubleType(), True),
            StructField("wind_deg", IntegerType(), True),
            StructField("clouds", IntegerType(), True),
            StructField("weather_main", StringType(), True),
            StructField("weather_description", StringType(), True),
            StructField("sunrise", StringType(), True),
            StructField("sunset", StringType(), True),
            StructField("ingestion_timestamp", StringType(), False),
            StructField("_kafka_partition", IntegerType(), True),
            StructField("_kafka_offset", IntegerType(), True),
            StructField("_kafka_timestamp", StringType(), True),
        ])


class SilverProcessor:
    """Processeur de données Silver."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialise le processeur Silver.
        
        Args:
            config_path: Chemin vers le fichier de configuration
        """
        # Charger la configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Créer la session Spark
        self.spark = SparkConfig.create_spark_session()
        
        # Configuration des chemins
        minio_endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
        self.bronze_bucket = os.getenv('MINIO_BUCKET_BRONZE')
        self.silver_bucket = os.getenv('MINIO_BUCKET_SILVER')
        
        self.bronze_path = f"s3a://{self.bronze_bucket}/"
        self.silver_path = f"s3a://{self.silver_bucket}/"
        
        # Paramètres de filtrage
        self.min_temp = self.config['processing']['transformations']['filter_min_temperature']
        self.max_temp = self.config['processing']['transformations']['filter_max_temperature']
        
        logger.info("SilverProcessor initialisé")
    
    def read_bronze_data(self, date: Optional[str] = None) -> DataFrame:
        """
        Lit les données Bronze depuis S3/MinIO.
        
        Args:
            date: Date au format YYYY-MM-DD (si None, lit toutes les données)
            
        Returns:
            DataFrame Spark
        """
        try:
            if date:
                # Lire une partition spécifique
                dt = datetime.strptime(date, '%Y-%m-%d')
                path = f"{self.bronze_path}year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
            else:
                # Lire toutes les données
                path = self.bronze_path
            
            logger.info(f"Lecture des données Bronze depuis: {path}")
            
            df = self.spark.read \
                .schema(WeatherDataSchema.get_bronze_schema()) \
                .parquet(path)
            
            count = df.count()
            logger.info(f"{count} enregistrements lus depuis Bronze")
            
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors de la lecture des données Bronze: {e}")
            raise
    
    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Nettoie les données brutes.
        
        Args:
            df: DataFrame brut
            
        Returns:
            DataFrame nettoyé
        """
        logger.info("Début du nettoyage des données")
        
        # Convertir les timestamps en types appropriés
        df_clean = df \
            .withColumn("timestamp", F.to_timestamp("timestamp")) \
            .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp")) \
            .withColumn("sunrise", F.to_timestamp("sunrise")) \
            .withColumn("sunset", F.to_timestamp("sunset"))
        
        # Filtrer les valeurs nulles critiques
        if self.config['processing']['transformations']['filter_null_values']:
            df_clean = df_clean.filter(
                F.col("city").isNotNull() &
                F.col("timestamp").isNotNull() &
                F.col("temperature").isNotNull()
            )
        
        # Filtrer les températures aberrantes
        df_clean = df_clean.filter(
            (F.col("temperature") >= self.min_temp) &
            (F.col("temperature") <= self.max_temp)
        )
        
        # Supprimer les doublons (idempotence)
        df_clean = df_clean.dropDuplicates(["city", "timestamp"])
        
        # Ajouter des colonnes de métadonnées
        df_clean = df_clean \
            .withColumn("processing_timestamp", F.current_timestamp()) \
            .withColumn("data_quality_score", self._calculate_quality_score(df_clean))
        
        count_after = df_clean.count()
        logger.info(f"Nettoyage terminé: {count_after} enregistrements conservés")
        
        return df_clean
    
    def _calculate_quality_score(self, df: DataFrame) -> F.Column:
        """
        Calcule un score de qualité des données (0-100).
        
        Args:
            df: DataFrame
            
        Returns:
            Colonne avec le score de qualité
        """
        # Score basé sur la complétude des champs
        score = F.lit(100)
        
        # Déduire des points pour chaque champ null
        for col_name in ["feels_like", "pressure", "humidity", "visibility", "wind_speed"]:
            score = F.when(F.col(col_name).isNull(), score - 10).otherwise(score)
        
        return score
    
    def add_derived_columns(self, df: DataFrame) -> DataFrame:
        """
        Ajoute des colonnes calculées.
        
        Args:
            df: DataFrame nettoyé
            
        Returns:
            DataFrame enrichi
        """
        logger.info("Ajout de colonnes dérivées")
        
        df_enriched = df \
            .withColumn("year", F.year("timestamp")) \
            .withColumn("month", F.month("timestamp")) \
            .withColumn("day", F.dayofmonth("timestamp")) \
            .withColumn("hour", F.hour("timestamp")) \
            .withColumn("day_of_week", F.dayofweek("timestamp")) \
            .withColumn("week_of_year", F.weekofyear("timestamp")) \
            .withColumn("quarter", F.quarter("timestamp")) \
            .withColumn("is_weekend", F.when(F.col("day_of_week").isin([1, 7]), True).otherwise(False)) \
            .withColumn("temp_range", F.col("temp_max") - F.col("temp_min")) \
            .withColumn("heat_index", self._calculate_heat_index()) \
            .withColumn("is_day", 
                F.when(
                    (F.col("timestamp") >= F.col("sunrise")) & 
                    (F.col("timestamp") <= F.col("sunset")),
                    True
                ).otherwise(False)
            )
        
        return df_enriched
    
    def _calculate_heat_index(self) -> F.Column:
        """
        Calcule l'indice de chaleur ressenti.
        Formule simplifiée basée sur température et humidité.
        
        Returns:
            Colonne avec l'indice de chaleur
        """
        # Formule simplifiée (approximation)
        return F.col("temperature") + (0.5555 * (F.col("humidity") / 100 - 1))
    
    def create_aggregations(self, df: DataFrame) -> DataFrame:
        """
        Crée des agrégations temporelles.
        
        Args:
            df: DataFrame enrichi
            
        Returns:
            DataFrame agrégé
        """
        logger.info("Création des agrégations")
        
        # Agrégation journalière par ville
        daily_agg = df.groupBy("city", "country", "year", "month", "day") \
            .agg(
                F.avg("temperature").alias("avg_temperature"),
                F.min("temp_min").alias("min_temperature"),
                F.max("temp_max").alias("max_temperature"),
                F.avg("humidity").alias("avg_humidity"),
                F.avg("wind_speed").alias("avg_wind_speed"),
                F.avg("pressure").alias("avg_pressure"),
                F.count("*").alias("record_count"),
                F.max("processing_timestamp").alias("last_updated")
            ) \
            .withColumn("aggregation_level", F.lit("daily"))
        
        # Agrégation horaire par ville
        hourly_agg = df.groupBy("city", "country", "year", "month", "day", "hour") \
            .agg(
                F.avg("temperature").alias("avg_temperature"),
                F.min("temp_min").alias("min_temperature"),
                F.max("temp_max").alias("max_temperature"),
                F.avg("humidity").alias("avg_humidity"),
                F.avg("wind_speed").alias("avg_wind_speed"),
                F.avg("pressure").alias("avg_pressure"),
                F.count("*").alias("record_count"),
                F.max("processing_timestamp").alias("last_updated")
            ) \
            .withColumn("aggregation_level", F.lit("hourly"))
        
        return daily_agg, hourly_agg
    
    def write_silver_data(self, df: DataFrame, output_name: str):
        """
        Écrit les données Silver dans S3/MinIO.
        
        Args:
            df: DataFrame à écrire
            output_name: Nom du dataset de sortie
        """
        try:
            output_path = f"{self.silver_path}{output_name}"
            
            logger.info(f"Écriture des données Silver vers: {output_path}")
            
            df.write \
                .mode("append") \
                .partitionBy("year", "month") \
                .parquet(output_path)
            
            count = df.count()
            logger.info(f"{count} enregistrements écrits dans Silver/{output_name}")
            
        except Exception as e:
            logger.error(f"Erreur lors de l'écriture des données Silver: {e}")
            raise
    
    def process(self, date: Optional[str] = None):
        """
        Exécute le pipeline de processing complet.
        
        Args:
            date: Date à traiter (format YYYY-MM-DD)
        """
        try:
            logger.info(f"Début du processing Silver pour la date: {date or 'toutes'}")
            
            # 1. Lire les données Bronze
            df_bronze = self.read_bronze_data(date)
            
            # 2. Nettoyer les données
            df_clean = self.clean_data(df_bronze)
            
            # 3. Enrichir avec des colonnes dérivées
            df_enriched = self.add_derived_columns(df_clean)
            
            # 4. Écrire les données détaillées
            self.write_silver_data(df_enriched, "weather_cleaned")
            
            # 5. Créer et écrire les agrégations
            daily_agg, hourly_agg = self.create_aggregations(df_enriched)
            self.write_silver_data(daily_agg, "weather_daily_agg")
            self.write_silver_data(hourly_agg, "weather_hourly_agg")
            
            logger.info("Processing Silver terminé avec succès")
            
        except Exception as e:
            logger.error(f"Erreur dans le pipeline Silver: {e}")
            raise
    
    def cleanup(self):
        """Nettoie les ressources Spark."""
        logger.info("Fermeture de la session Spark")
        self.spark.stop()


def main():
    """Point d'entrée principal."""
    import argparse
    
    logger.add("logs/silver_processing_{time}.log", rotation="1 day", retention="7 days")
    
    parser = argparse.ArgumentParser(description="Processeur Silver pour données météo")
    parser.add_argument(
        "--date",
        type=str,
        help="Date à traiter au format YYYY-MM-DD (si omis, traite toutes les données)",
        default=None
    )
    
    args = parser.parse_args()
    
    try:
        processor = SilverProcessor()
        processor.process(args.date)
        processor.cleanup()
        
    except Exception as e:
        logger.error(f"Erreur fatale dans le processeur Silver: {e}")
        raise


if __name__ == "__main__":
    main()
