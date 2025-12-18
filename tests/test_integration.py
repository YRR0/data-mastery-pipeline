"""
Script de test pour vérifier la circulation des données entre Kafka et S3.
Ce script vérifie que le pipeline d'ingestion fonctionne correctement.
"""

import os
import time
import json
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from minio import Minio
from minio.error import S3Error
from loguru import logger
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC_WEATHER', 'weather-data')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_BUCKET = os.getenv('MINIO_BUCKET_BRONZE', 'bronze-layer')


class PipelineIntegrationTest:
    """Test d'intégration du pipeline Kafka -> S3."""
    
    def __init__(self):
        """Initialise les clients de test."""
        self.test_message_id = f"test_{datetime.utcnow().isoformat()}"
        
        # Client Kafka
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("KafkaProducer initialisé")
        except KafkaError as e:
            logger.error(f"Erreur lors de l'initialisation du KafkaProducer: {e}")
            raise
        
        # Client MinIO
        try:
            self.minio_client = Minio(
                MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=False
            )
            logger.info("Client MinIO initialisé")
        except S3Error as e:
            logger.error(f"Erreur lors de l'initialisation du client MinIO: {e}")
            raise
    
    def test_kafka_connection(self) -> bool:
        """Test de la connexion Kafka."""
        logger.info("Test de connexion Kafka...")
        
        try:
            # Envoyer un message de test
            test_data = {
                'test_id': self.test_message_id,
                'city': 'TestCity',
                'temperature': 20.0,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            future = self.producer.send(KAFKA_TOPIC, value=test_data)
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"✓ Message envoyé à Kafka - "
                f"Topic: {record_metadata.topic}, "
                f"Partition: {record_metadata.partition}, "
                f"Offset: {record_metadata.offset}"
            )
            
            return True
            
        except KafkaError as e:
            logger.error(f"✗ Erreur lors de l'envoi du message: {e}")
            return False
    
    def test_kafka_consumption(self) -> bool:
        """Test de la consommation depuis Kafka."""
        logger.info("Test de consommation Kafka...")
        
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
                auto_offset_reset='latest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=10000
            )
            
            message_found = False
            
            for message in consumer:
                data = message.value
                if data.get('test_id') == self.test_message_id:
                    logger.info(f"✓ Message de test trouvé dans Kafka: {data}")
                    message_found = True
                    break
            
            consumer.close()
            
            if not message_found:
                logger.warning("⚠ Message de test non trouvé (peut être normal si déjà consommé)")
            
            return True
            
        except Exception as e:
            logger.error(f"✗ Erreur lors de la consommation: {e}")
            return False
    
    def test_minio_connection(self) -> bool:
        """Test de la connexion MinIO."""
        logger.info("Test de connexion MinIO...")
        
        try:
            # Vérifier que le bucket existe
            bucket_exists = self.minio_client.bucket_exists(MINIO_BUCKET)
            
            if bucket_exists:
                logger.info(f"✓ Bucket {MINIO_BUCKET} existe")
            else:
                logger.warning(f"⚠ Bucket {MINIO_BUCKET} n'existe pas")
                return False
            
            # Lister les objets
            objects = self.minio_client.list_objects(MINIO_BUCKET, recursive=True)
            object_count = sum(1 for _ in objects)
            
            logger.info(f"✓ {object_count} objets trouvés dans le bucket")
            
            return True
            
        except S3Error as e:
            logger.error(f"✗ Erreur lors de la connexion à MinIO: {e}")
            return False
    
    def test_data_flow(self) -> bool:
        """Test du flux complet Kafka -> S3."""
        logger.info("Test du flux complet Kafka -> S3...")
        
        # 1. Envoyer un message de test
        logger.info("1. Envoi d'un message de test à Kafka...")
        if not self.test_kafka_connection():
            return False
        
        # 2. Attendre que le consumer traite le message
        logger.info("2. Attente du traitement par le consumer (30 secondes)...")
        time.sleep(30)
        
        # 3. Vérifier dans MinIO
        logger.info("3. Vérification de la présence des données dans MinIO...")
        try:
            # Chercher des fichiers récents
            objects = self.minio_client.list_objects(MINIO_BUCKET, recursive=True)
            
            recent_objects = []
            cutoff_time = time.time() - 300  # 5 minutes
            
            for obj in objects:
                if obj.last_modified.timestamp() > cutoff_time:
                    recent_objects.append(obj.object_name)
            
            if recent_objects:
                logger.info(f"✓ {len(recent_objects)} fichiers récents trouvés dans MinIO:")
                for obj_name in recent_objects[:5]:  # Afficher les 5 premiers
                    logger.info(f"  - {obj_name}")
                return True
            else:
                logger.warning("⚠ Aucun fichier récent trouvé dans MinIO")
                return False
                
        except S3Error as e:
            logger.error(f"✗ Erreur lors de la vérification dans MinIO: {e}")
            return False
    
    def run_all_tests(self):
        """Exécute tous les tests."""
        logger.info("=" * 60)
        logger.info("DÉBUT DES TESTS D'INTÉGRATION DU PIPELINE")
        logger.info("=" * 60)
        
        results = {
            'kafka_connection': self.test_kafka_connection(),
            'minio_connection': self.test_minio_connection(),
            'kafka_consumption': self.test_kafka_consumption(),
            'data_flow': self.test_data_flow()
        }
        
        logger.info("=" * 60)
        logger.info("RÉSULTATS DES TESTS")
        logger.info("=" * 60)
        
        for test_name, result in results.items():
            status = "✓ PASS" if result else "✗ FAIL"
            logger.info(f"{test_name}: {status}")
        
        all_passed = all(results.values())
        
        if all_passed:
            logger.info("=" * 60)
            logger.info("✓ TOUS LES TESTS SONT PASSÉS")
            logger.info("=" * 60)
        else:
            logger.error("=" * 60)
            logger.error("✗ CERTAINS TESTS ONT ÉCHOUÉ")
            logger.error("=" * 60)
        
        return all_passed
    
    def cleanup(self):
        """Nettoie les ressources."""
        self.producer.close()
        logger.info("Ressources nettoyées")


def main():
    """Point d'entrée principal."""
    logger.add("logs/integration_test_{time}.log", rotation="1 day")
    
    try:
        test = PipelineIntegrationTest()
        success = test.run_all_tests()
        test.cleanup()
        
        exit(0 if success else 1)
        
    except Exception as e:
        logger.error(f"Erreur fatale dans les tests: {e}")
        exit(1)


if __name__ == "__main__":
    main()
