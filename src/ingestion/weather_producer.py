"""
Module d'ingestion de données météo vers Kafka.
Ce script extrait des données depuis l'API OpenWeather et les envoie vers un topic Kafka.
"""

import json
import time
from datetime import datetime
from typing import Dict, List, Optional
import os
from dataclasses import dataclass, asdict

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from loguru import logger
from dotenv import load_dotenv
import yaml

# Charger les variables d'environnement
load_dotenv()


@dataclass
class WeatherData:
    """Structure de données météo."""
    city: str
    country: str
    timestamp: str
    temperature: float
    feels_like: float
    temp_min: float
    temp_max: float
    pressure: int
    humidity: int
    visibility: int
    wind_speed: float
    wind_deg: int
    clouds: int
    weather_main: str
    weather_description: str
    sunrise: str
    sunset: str
    ingestion_timestamp: str

    def to_dict(self) -> Dict:
        """Convertit l'objet en dictionnaire."""
        return asdict(self)


class WeatherAPIClient:
    """Client pour interagir avec l'API OpenWeather."""
    
    def __init__(self, api_key: str, base_url: str):
        """
        Initialise le client API.
        
        Args:
            api_key: Clé API OpenWeather
            base_url: URL de base de l'API
        """
        self.api_key = api_key
        self.base_url = base_url
        logger.info("WeatherAPIClient initialisé")
    
    def fetch_weather(self, city: str) -> Optional[Dict]:
        """
        Récupère les données météo pour une ville.
        
        Args:
            city: Nom de la ville
            
        Returns:
            Dictionnaire avec les données météo ou None en cas d'erreur
        """
        try:
            params = {
                'q': city,
                'appid': self.api_key,
                'units': 'metric'  # Celsius
            }
            
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            logger.info(f"Données météo récupérées avec succès pour {city}")
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur lors de la récupération des données pour {city}: {e}")
            return None
    
    def parse_weather_data(self, raw_data: Dict) -> Optional[WeatherData]:
        """
        Parse les données brutes de l'API en objet WeatherData.
        
        Args:
            raw_data: Données brutes de l'API
            
        Returns:
            Objet WeatherData ou None
        """
        try:
            weather_data = WeatherData(
                city=raw_data['name'],
                country=raw_data['sys']['country'],
                timestamp=datetime.fromtimestamp(raw_data['dt']).isoformat(),
                temperature=raw_data['main']['temp'],
                feels_like=raw_data['main']['feels_like'],
                temp_min=raw_data['main']['temp_min'],
                temp_max=raw_data['main']['temp_max'],
                pressure=raw_data['main']['pressure'],
                humidity=raw_data['main']['humidity'],
                visibility=raw_data.get('visibility', 0),
                wind_speed=raw_data['wind']['speed'],
                wind_deg=raw_data['wind']['deg'],
                clouds=raw_data['clouds']['all'],
                weather_main=raw_data['weather'][0]['main'],
                weather_description=raw_data['weather'][0]['description'],
                sunrise=datetime.fromtimestamp(raw_data['sys']['sunrise']).isoformat(),
                sunset=datetime.fromtimestamp(raw_data['sys']['sunset']).isoformat(),
                ingestion_timestamp=datetime.utcnow().isoformat()
            )
            
            return weather_data
            
        except (KeyError, IndexError) as e:
            logger.error(f"Erreur lors du parsing des données: {e}")
            return None


class WeatherKafkaProducer:
    """Producer Kafka pour les données météo."""
    
    def __init__(self, bootstrap_servers: str, topic: str):
        """
        Initialise le producer Kafka.
        
        Args:
            bootstrap_servers: Serveurs Kafka
            topic: Topic Kafka de destination
        """
        self.topic = topic
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers.split(','),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                compression_type='gzip',
                acks='all',  # Attendre l'acknowledgement de tous les réplicas
                retries=3,
                max_in_flight_requests_per_connection=1  # Garantir l'ordre
            )
            logger.info(f"KafkaProducer initialisé pour le topic {topic}")
            
        except KafkaError as e:
            logger.error(f"Erreur lors de l'initialisation du KafkaProducer: {e}")
            raise
    
    def send_weather_data(self, weather_data: WeatherData) -> bool:
        """
        Envoie les données météo vers Kafka.
        
        Args:
            weather_data: Données météo à envoyer
            
        Returns:
            True si envoi réussi, False sinon
        """
        try:
            # Utiliser la ville comme clé pour garantir l'ordre par ville
            key = weather_data.city
            value = weather_data.to_dict()
            
            future = self.producer.send(self.topic, key=key, value=value)
            
            # Attendre la confirmation (bloquant)
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"Message envoyé avec succès - "
                f"Topic: {record_metadata.topic}, "
                f"Partition: {record_metadata.partition}, "
                f"Offset: {record_metadata.offset}"
            )
            
            return True
            
        except KafkaError as e:
            logger.error(f"Erreur lors de l'envoi du message Kafka: {e}")
            return False
    
    def close(self):
        """Ferme proprement le producer."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("KafkaProducer fermé")


class WeatherIngestionPipeline:
    """Pipeline d'ingestion de données météo."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialise le pipeline d'ingestion.
        
        Args:
            config_path: Chemin vers le fichier de configuration
        """
        # Charger la configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialiser les composants
        api_key = os.getenv('OPENWEATHER_API_KEY')
        base_url = os.getenv('OPENWEATHER_BASE_URL')
        
        if not api_key:
            raise ValueError("OPENWEATHER_API_KEY non définie dans les variables d'environnement")
        
        self.api_client = WeatherAPIClient(api_key, base_url)
        
        kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        kafka_topic = os.getenv('KAFKA_TOPIC_WEATHER', 'weather-data')
        
        self.kafka_producer = WeatherKafkaProducer(kafka_servers, kafka_topic)
        
        self.cities = self.config['ingestion']['cities']
        self.fetch_interval = self.config['ingestion']['fetch_interval_seconds']
        
        logger.info("WeatherIngestionPipeline initialisé")
    
    def run_once(self) -> int:
        """
        Exécute une itération du pipeline (toutes les villes).
        
        Returns:
            Nombre de messages envoyés avec succès
        """
        success_count = 0
        
        logger.info(f"Début de l'ingestion pour {len(self.cities)} villes")
        
        for city in self.cities:
            try:
                # Récupérer les données
                raw_data = self.api_client.fetch_weather(city)
                
                if raw_data:
                    # Parser les données
                    weather_data = self.api_client.parse_weather_data(raw_data)
                    
                    if weather_data:
                        # Envoyer vers Kafka
                        if self.kafka_producer.send_weather_data(weather_data):
                            success_count += 1
                
                # Petit délai entre les appels API
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Erreur lors du traitement de {city}: {e}")
        
        logger.info(f"Ingestion terminée - {success_count}/{len(self.cities)} messages envoyés")
        
        return success_count
    
    def run_continuous(self):
        """Exécute le pipeline en continu avec l'intervalle configuré."""
        logger.info(f"Démarrage du mode continu (intervalle: {self.fetch_interval}s)")
        
        try:
            while True:
                self.run_once()
                logger.info(f"Attente de {self.fetch_interval} secondes avant la prochaine itération")
                time.sleep(self.fetch_interval)
                
        except KeyboardInterrupt:
            logger.info("Arrêt demandé par l'utilisateur")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Nettoie les ressources."""
        logger.info("Nettoyage des ressources")
        self.kafka_producer.close()


def main():
    """Point d'entrée principal."""
    logger.add("logs/ingestion_{time}.log", rotation="1 day", retention="7 days")
    
    try:
        pipeline = WeatherIngestionPipeline()
        
        # Exécuter une seule fois ou en continu selon la variable d'environnement
        mode = os.getenv('INGESTION_MODE', 'once')
        
        if mode == 'continuous':
            pipeline.run_continuous()
        else:
            pipeline.run_once()
            pipeline.cleanup()
            
    except Exception as e:
        logger.error(f"Erreur fatale dans le pipeline d'ingestion: {e}")
        raise


if __name__ == "__main__":
    main()
