"""
Tests unitaires et d'intégration pour le pipeline de données.
"""

import pytest
import os
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer

# Configuration pour les tests
os.environ['OPENWEATHER_API_KEY'] = 'test_api_key'
os.environ['OPENWEATHER_BASE_URL'] = 'https://api.openweathermap.org/data/2.5/weather'
os.environ['KAFKA_BOOTSTRAP_SERVERS'] = 'localhost:9092'
os.environ['KAFKA_TOPIC_WEATHER'] = 'weather-data-test'
os.environ['MINIO_ENDPOINT'] = 'localhost:9000'
os.environ['MINIO_ACCESS_KEY'] = 'minioadmin'
os.environ['MINIO_SECRET_KEY'] = 'minioadmin'
os.environ['MINIO_BUCKET_BRONZE'] = 'bronze-layer-test'
os.environ['MINIO_BUCKET_SILVER'] = 'silver-layer-test'


# =====================================================
# Tests pour l'ingestion (weather_producer.py)
# =====================================================

class TestWeatherAPIClient:
    """Tests pour le client API météo."""
    
    @pytest.fixture
    def api_client(self):
        """Fixture pour créer un client API."""
        from src.ingestion.weather_producer import WeatherAPIClient
        return WeatherAPIClient('test_key', 'https://test.api.com')
    
    @patch('src.ingestion.weather_producer.requests.get')
    def test_fetch_weather_success(self, mock_get, api_client):
        """Test de récupération réussie des données météo."""
        # Mock de la réponse API
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'name': 'Paris',
            'sys': {'country': 'FR', 'sunrise': 1234567890, 'sunset': 1234567900},
            'dt': 1234567890,
            'main': {
                'temp': 20.5,
                'feels_like': 19.0,
                'temp_min': 18.0,
                'temp_max': 22.0,
                'pressure': 1013,
                'humidity': 65
            },
            'visibility': 10000,
            'wind': {'speed': 5.5, 'deg': 180},
            'clouds': {'all': 20},
            'weather': [{'main': 'Clear', 'description': 'clear sky'}]
        }
        mock_get.return_value = mock_response
        
        # Tester la récupération
        result = api_client.fetch_weather('Paris')
        
        assert result is not None
        assert result['name'] == 'Paris'
        assert result['main']['temp'] == 20.5
    
    @patch('src.ingestion.weather_producer.requests.get')
    def test_fetch_weather_failure(self, mock_get, api_client):
        """Test de gestion d'erreur API."""
        mock_get.side_effect = Exception("API Error")
        
        result = api_client.fetch_weather('InvalidCity')
        
        assert result is None
    
    def test_parse_weather_data(self, api_client):
        """Test du parsing des données météo."""
        raw_data = {
            'name': 'London',
            'sys': {'country': 'GB', 'sunrise': 1234567890, 'sunset': 1234567900},
            'dt': 1234567890,
            'main': {
                'temp': 15.0,
                'feels_like': 14.0,
                'temp_min': 13.0,
                'temp_max': 17.0,
                'pressure': 1015,
                'humidity': 70
            },
            'visibility': 8000,
            'wind': {'speed': 3.5, 'deg': 90},
            'clouds': {'all': 50},
            'weather': [{'main': 'Clouds', 'description': 'scattered clouds'}]
        }
        
        weather_data = api_client.parse_weather_data(raw_data)
        
        assert weather_data is not None
        assert weather_data.city == 'London'
        assert weather_data.temperature == 15.0
        assert weather_data.humidity == 70


class TestWeatherKafkaProducer:
    """Tests pour le producer Kafka."""
    
    @pytest.fixture
    def mock_producer(self):
        """Fixture pour un mock de KafkaProducer."""
        with patch('src.ingestion.weather_producer.KafkaProducer') as mock:
            yield mock
    
    def test_send_weather_data(self, mock_producer):
        """Test d'envoi de données vers Kafka."""
        from src.ingestion.weather_producer import WeatherKafkaProducer, WeatherData
        
        # Créer des données de test
        weather_data = WeatherData(
            city='Paris',
            country='FR',
            timestamp='2024-01-01T12:00:00',
            temperature=20.0,
            feels_like=19.0,
            temp_min=18.0,
            temp_max=22.0,
            pressure=1013,
            humidity=65,
            visibility=10000,
            wind_speed=5.5,
            wind_deg=180,
            clouds=20,
            weather_main='Clear',
            weather_description='clear sky',
            sunrise='2024-01-01T07:00:00',
            sunset='2024-01-01T17:00:00',
            ingestion_timestamp='2024-01-01T12:00:00'
        )
        
        # Mock du future
        mock_future = Mock()
        mock_future.get.return_value = Mock(
            topic='weather-data-test',
            partition=0,
            offset=123
        )
        mock_producer.return_value.send.return_value = mock_future
        
        producer = WeatherKafkaProducer('localhost:9092', 'weather-data-test')
        result = producer.send_weather_data(weather_data)
        
        assert result is True


# =====================================================
# Tests pour le processing Silver
# =====================================================

class TestSilverProcessor:
    """Tests pour le processeur Silver."""
    
    @pytest.fixture
    def sample_bronze_data(self):
        """Fixture pour créer des données Bronze de test."""
        return pd.DataFrame([
            {
                'city': 'Paris',
                'country': 'FR',
                'timestamp': '2024-01-01T12:00:00',
                'temperature': 20.0,
                'feels_like': 19.0,
                'temp_min': 18.0,
                'temp_max': 22.0,
                'pressure': 1013,
                'humidity': 65,
                'visibility': 10000,
                'wind_speed': 5.5,
                'wind_deg': 180,
                'clouds': 20,
                'weather_main': 'Clear',
                'weather_description': 'clear sky',
                'sunrise': '2024-01-01T07:00:00',
                'sunset': '2024-01-01T17:00:00',
                'ingestion_timestamp': '2024-01-01T12:00:00',
                '_kafka_partition': 0,
                '_kafka_offset': 123,
                '_kafka_timestamp': '2024-01-01T12:00:00'
            },
            {
                'city': 'London',
                'country': 'GB',
                'timestamp': '2024-01-01T12:00:00',
                'temperature': 15.0,
                'feels_like': 14.0,
                'temp_min': 13.0,
                'temp_max': 17.0,
                'pressure': 1015,
                'humidity': 70,
                'visibility': 8000,
                'wind_speed': 3.5,
                'wind_deg': 90,
                'clouds': 50,
                'weather_main': 'Clouds',
                'weather_description': 'scattered clouds',
                'sunrise': '2024-01-01T07:30:00',
                'sunset': '2024-01-01T16:30:00',
                'ingestion_timestamp': '2024-01-01T12:00:00',
                '_kafka_partition': 0,
                '_kafka_offset': 124,
                '_kafka_timestamp': '2024-01-01T12:00:00'
            }
        ])
    
    def test_data_validation(self, sample_bronze_data):
        """Test de validation des données."""
        # Vérifier qu'il n'y a pas de valeurs nulles critiques
        assert sample_bronze_data['city'].notna().all()
        assert sample_bronze_data['temperature'].notna().all()
        
        # Vérifier les plages de température
        assert (sample_bronze_data['temperature'] >= -100).all()
        assert (sample_bronze_data['temperature'] <= 60).all()
    
    def test_deduplication(self, sample_bronze_data):
        """Test de la déduplication."""
        # Ajouter un doublon
        df_with_dup = pd.concat([sample_bronze_data, sample_bronze_data.iloc[[0]]])
        
        # Dédupliquer
        df_clean = df_with_dup.drop_duplicates(subset=['city', 'timestamp'])
        
        assert len(df_clean) == len(sample_bronze_data)


# =====================================================
# Tests pour le chargement Gold
# =====================================================

class TestGoldLoader:
    """Tests pour le loader Gold."""
    
    @pytest.fixture
    def mock_db_connection(self):
        """Fixture pour mock de connexion PostgreSQL."""
        with patch('src.loading.gold_loader.psycopg2.connect') as mock:
            yield mock
    
    def test_pipeline_logging(self, mock_db_connection):
        """Test du logging du pipeline."""
        from src.loading.gold_loader import PipelineLogger, PostgreSQLConnection
        
        # Mock de la connexion
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_connection.return_value = mock_conn
        
        db_conn = PostgreSQLConnection()
        db_conn.connect()
        
        logger = PipelineLogger(db_conn)
        
        # Test log_start
        logger.log_start('test_pipeline', 'test_stage')
        
        # Vérifier que execute a été appelé
        assert mock_cursor.execute.called


# =====================================================
# Tests d'intégration
# =====================================================

@pytest.mark.integration
class TestPipelineIntegration:
    """Tests d'intégration du pipeline complet."""
    
    def test_end_to_end_flow(self):
        """Test du flux end-to-end (nécessite Docker)."""
        # Ce test nécessite que les services Docker soient démarrés
        # Il serait exécuté dans un environnement CI/CD
        
        # 1. Ingérer des données
        # 2. Vérifier dans Kafka
        # 3. Vérifier dans MinIO (Bronze)
        # 4. Vérifier le processing (Silver)
        # 5. Vérifier le chargement (Gold)
        
        pytest.skip("Test d'intégration nécessitant Docker")


# =====================================================
# Tests de qualité des données
# =====================================================

class TestDataQuality:
    """Tests de qualité des données."""
    
    def test_temperature_range(self):
        """Test de la plage de température."""
        df = pd.DataFrame({
            'temperature': [20.0, 15.0, -50.0, 70.0, 10.0]
        })
        
        # Filtrer les températures aberrantes
        df_clean = df[(df['temperature'] >= -100) & (df['temperature'] <= 60)]
        
        assert len(df_clean) == 4  # 70.0 devrait être filtré
    
    def test_null_percentage(self):
        """Test du pourcentage de valeurs nulles."""
        df = pd.DataFrame({
            'city': ['Paris', 'London', None, 'Tokyo'],
            'temperature': [20.0, None, 25.0, 18.0]
        })
        
        null_pct = df['city'].isna().sum() / len(df)
        
        assert null_pct <= 0.1  # Max 10% de nulls


# =====================================================
# Configuration pytest
# =====================================================

def pytest_configure(config):
    """Configuration de pytest."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
