"""
Consumer Kafka qui sauvegarde les messages dans S3/MinIO (couche Bronze).
Ce script lit les messages du topic Kafka et les stocke au format Parquet.
"""

import json
import os
from datetime import datetime
from typing import List, Dict
import io

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from minio import Minio
from minio.error import S3Error
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger
from dotenv import load_dotenv
import yaml

# Charger les variables d'environnement
load_dotenv()


class MinIOClient:
    """Client pour interagir avec MinIO/S3."""
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, secure: bool = False):
        """
        Initialise le client MinIO.
        
        Args:
            endpoint: URL du serveur MinIO
            access_key: Clé d'accès
            secret_key: Clé secrète
            secure: Utiliser HTTPS
        """
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        logger.info(f"Client MinIO initialisé pour {endpoint}")
    
    def ensure_bucket_exists(self, bucket_name: str):
        """
        Vérifie que le bucket existe, sinon le crée.
        
        Args:
            bucket_name: Nom du bucket
        """
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                logger.info(f"Bucket {bucket_name} créé")
            else:
                logger.info(f"Bucket {bucket_name} existe déjà")
        except S3Error as e:
            logger.error(f"Erreur lors de la vérification/création du bucket: {e}")
            raise
    
    def upload_parquet(self, bucket_name: str, object_name: str, df: pd.DataFrame):
        """
        Upload un DataFrame sous forme de fichier Parquet.
        
        Args:
            bucket_name: Nom du bucket
            object_name: Chemin de l'objet (clé S3)
            df: DataFrame à sauvegarder
        """
        try:
            # Convertir le DataFrame en Parquet en mémoire
            buffer = io.BytesIO()
            
            # Utiliser PyArrow pour la compression Snappy
            table = pa.Table.from_pandas(df)
            pq.write_table(
                table, 
                buffer, 
                compression='snappy',
                use_dictionary=True,
                write_statistics=True
            )
            
            buffer.seek(0)
            
            # Upload vers MinIO
            self.client.put_object(
                bucket_name,
                object_name,
                buffer,
                length=buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
            
            logger.info(f"Fichier Parquet uploadé: {bucket_name}/{object_name}")
            
        except S3Error as e:
            logger.error(f"Erreur lors de l'upload du fichier Parquet: {e}")
            raise


class KafkaToS3Consumer:
    """Consumer Kafka qui sauvegarde dans S3/MinIO."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialise le consumer.
        
        Args:
            config_path: Chemin vers le fichier de configuration
        """
        # Charger la configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Configuration Kafka
        kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        kafka_topic = os.getenv('KAFKA_TOPIC_WEATHER', 'weather-data')
        consumer_group = self.config['kafka']['consumer_group']
        
        # Initialiser le consumer Kafka
        try:
            self.consumer = KafkaConsumer(
                kafka_topic,
                bootstrap_servers=kafka_servers.split(','),
                group_id=consumer_group,
                auto_offset_reset=self.config['kafka']['auto_offset_reset'],
                enable_auto_commit=False,  # Commit manuel pour garantir la sauvegarde
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                max_poll_records=self.config['ingestion']['batch_size']
            )
            logger.info(f"KafkaConsumer initialisé pour le topic {kafka_topic}")
            
        except KafkaError as e:
            logger.error(f"Erreur lors de l'initialisation du KafkaConsumer: {e}")
            raise
        
        # Configuration MinIO
        minio_config = self.config['storage']['minio']
        self.minio_client = MinIOClient(
            endpoint=os.getenv('MINIO_ENDPOINT'),
            access_key=os.getenv('MINIO_ACCESS_KEY'),
            secret_key=os.getenv('MINIO_SECRET_KEY'),
            secure=os.getenv('MINIO_SECURE', 'false').lower() == 'true'
        )
        
        self.bronze_bucket = os.getenv('MINIO_BUCKET_BRONZE')
        self.minio_client.ensure_bucket_exists(self.bronze_bucket)
        
        self.batch_size = self.config['ingestion']['batch_size']
        self.buffer: List[Dict] = []
        
        logger.info("KafkaToS3Consumer initialisé")
    
    def generate_s3_key(self, timestamp: str) -> str:
        """
        Génère une clé S3 partitionnée par date.
        
        Args:
            timestamp: Timestamp ISO 8601
            
        Returns:
            Clé S3 au format: year=YYYY/month=MM/day=DD/weather_YYYYMMDD_HHMMSS.parquet
        """
        dt = datetime.fromisoformat(timestamp)
        
        partition_path = f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
        filename = f"weather_{dt.strftime('%Y%m%d_%H%M%S')}_{dt.microsecond}.parquet"
        
        return f"{partition_path}/{filename}"
    
    def save_batch_to_s3(self):
        """Sauvegarde le batch actuel dans S3."""
        if not self.buffer:
            logger.warning("Buffer vide, aucune sauvegarde effectuée")
            return
        
        try:
            # Convertir en DataFrame
            df = pd.DataFrame(self.buffer)
            
            # Générer la clé S3 basée sur le timestamp du premier message
            first_timestamp = self.buffer[0]['ingestion_timestamp']
            s3_key = self.generate_s3_key(first_timestamp)
            
            # Upload vers MinIO
            self.minio_client.upload_parquet(self.bronze_bucket, s3_key, df)
            
            logger.info(f"Batch de {len(self.buffer)} messages sauvegardé dans {s3_key}")
            
            # Vider le buffer
            self.buffer.clear()
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde du batch: {e}")
            raise
    
    def process_message(self, message):
        """
        Traite un message Kafka.
        
        Args:
            message: Message Kafka
        """
        try:
            data = message.value
            
            # Ajouter des métadonnées Kafka
            data['_kafka_partition'] = message.partition
            data['_kafka_offset'] = message.offset
            data['_kafka_timestamp'] = datetime.fromtimestamp(
                message.timestamp / 1000
            ).isoformat()
            
            self.buffer.append(data)
            
            logger.debug(f"Message ajouté au buffer (taille: {len(self.buffer)})")
            
        except Exception as e:
            logger.error(f"Erreur lors du traitement du message: {e}")
    
    def run(self):
        """Exécute le consumer en continu."""
        logger.info("Démarrage de la consommation des messages Kafka")
        
        try:
            for message in self.consumer:
                self.process_message(message)
                
                # Sauvegarder quand le batch est plein
                if len(self.buffer) >= self.batch_size:
                    self.save_batch_to_s3()
                    
                    # Commit manuel après sauvegarde réussie (idempotence)
                    self.consumer.commit()
                    logger.info("Offsets Kafka commités")
                
        except KeyboardInterrupt:
            logger.info("Arrêt demandé par l'utilisateur")
        except Exception as e:
            logger.error(f"Erreur dans la boucle de consommation: {e}")
            raise
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Nettoie les ressources."""
        logger.info("Nettoyage des ressources")
        
        # Sauvegarder les messages restants dans le buffer
        if self.buffer:
            logger.info(f"Sauvegarde des {len(self.buffer)} messages restants")
            try:
                self.save_batch_to_s3()
                self.consumer.commit()
            except Exception as e:
                logger.error(f"Erreur lors de la sauvegarde finale: {e}")
        
        # Fermer le consumer
        self.consumer.close()
        logger.info("Consumer Kafka fermé")


def main():
    """Point d'entrée principal."""
    logger.add("logs/consumer_{time}.log", rotation="1 day", retention="7 days")
    
    try:
        consumer = KafkaToS3Consumer()
        consumer.run()
        
    except Exception as e:
        logger.error(f"Erreur fatale dans le consumer: {e}")
        raise


if __name__ == "__main__":
    main()
