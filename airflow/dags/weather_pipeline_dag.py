"""
DAG Airflow pour orchestrer le pipeline de données météo end-to-end.
Ce DAG gère l'ingestion, le processing et le chargement des données.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor
from airflow.utils.dates import days_ago

# Configuration par défaut du DAG
default_args = {
    'owner': 'dataeng',
    'depends_on_past': False,
    'email': ['dataeng@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Définition du DAG
dag = DAG(
    dag_id='weather_pipeline',
    default_args=default_args,
    description='Pipeline ETL complet pour les données météorologiques',
    schedule_interval='0 * * * *',  # Toutes les heures
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Ne pas exécuter les runs manqués
    max_active_runs=1,  # Une seule exécution à la fois
    tags=['weather', 'etl', 'production'],
)


def check_api_availability(**context):
    """Vérifie que l'API OpenWeather est accessible."""
    import requests
    import os
    from loguru import logger
    
    api_key = os.getenv('OPENWEATHER_API_KEY')
    base_url = os.getenv('OPENWEATHER_BASE_URL')
    
    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY non définie")
    
    # Test avec une ville
    try:
        response = requests.get(
            base_url,
            params={'q': 'Paris', 'appid': api_key},
            timeout=10
        )
        response.raise_for_status()
        logger.info("API OpenWeather accessible")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"API OpenWeather inaccessible: {e}")
        raise


def run_ingestion(**context):
    """Exécute l'ingestion des données météo vers Kafka."""
    import sys
    sys.path.insert(0, '/opt/airflow/src')
    
    from ingestion.weather_producer import WeatherIngestionPipeline
    from loguru import logger
    
    logger.info("Démarrage de l'ingestion des données")
    
    pipeline = WeatherIngestionPipeline()
    success_count = pipeline.run_once()
    pipeline.cleanup()
    
    logger.info(f"Ingestion terminée: {success_count} messages envoyés")
    
    # Pousser les métriques dans XCom
    context['ti'].xcom_push(key='ingestion_count', value=success_count)
    
    return success_count


def run_kafka_to_s3(**context):
    """Exécute le consumer Kafka pour sauvegarder dans S3."""
    import sys
    sys.path.insert(0, '/opt/airflow/src')
    
    from ingestion.kafka_consumer_s3 import KafkaToS3Consumer
    from loguru import logger
    import time
    
    logger.info("Démarrage du consumer Kafka vers S3")
    
    consumer = KafkaToS3Consumer()
    
    # Consumer pour une durée limitée (5 minutes max)
    start_time = time.time()
    timeout = 300  # 5 minutes
    
    try:
        for message in consumer.consumer:
            consumer.process_message(message)
            
            # Sauvegarder par batch
            if len(consumer.buffer) >= consumer.batch_size:
                consumer.save_batch_to_s3()
                consumer.consumer.commit()
            
            # Vérifier le timeout
            if time.time() - start_time > timeout:
                logger.info("Timeout atteint, arrêt du consumer")
                break
        
        # Sauvegarder les messages restants
        if consumer.buffer:
            consumer.save_batch_to_s3()
            consumer.consumer.commit()
        
    finally:
        consumer.cleanup()
    
    logger.info("Consumer Kafka vers S3 terminé")


def run_silver_processing(**context):
    """Exécute le processing Silver avec PySpark."""
    import sys
    sys.path.insert(0, '/opt/airflow/src')
    
    from processing.silver_processor import SilverProcessor
    from loguru import logger
    
    logger.info("Démarrage du processing Silver")
    
    # Traiter les données d'aujourd'hui
    today = context['ds']  # Format YYYY-MM-DD
    
    processor = SilverProcessor()
    processor.process(date=today)
    processor.cleanup()
    
    logger.info("Processing Silver terminé")


def run_gold_loading(**context):
    """Exécute le chargement vers PostgreSQL (Gold)."""
    import sys
    sys.path.insert(0, '/opt/airflow/src')
    
    from loading.gold_loader import GoldLoader
    from loguru import logger
    
    logger.info("Démarrage du chargement vers Gold")
    
    loader = GoldLoader()
    loader.load_all()
    loader.cleanup()
    
    logger.info("Chargement vers Gold terminé")


def data_quality_check(**context):
    """Vérifie la qualité des données chargées."""
    import psycopg2
    import os
    from loguru import logger
    
    logger.info("Vérification de la qualité des données")
    
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )
    
    cursor = conn.cursor()
    
    # Vérifier le nombre d'enregistrements
    cursor.execute("SELECT COUNT(*) FROM gold_weather_detailed")
    count = cursor.fetchone()[0]
    logger.info(f"Nombre total d'enregistrements: {count}")
    
    if count == 0:
        raise ValueError("Aucun enregistrement trouvé dans gold_weather_detailed")
    
    # Vérifier les doublons
    cursor.execute("""
        SELECT city, timestamp, COUNT(*)
        FROM gold_weather_detailed
        GROUP BY city, timestamp
        HAVING COUNT(*) > 1
    """)
    duplicates = cursor.fetchall()
    
    if duplicates:
        logger.warning(f"Doublons détectés: {len(duplicates)}")
    
    # Vérifier les valeurs nulles critiques
    cursor.execute("""
        SELECT COUNT(*)
        FROM gold_weather_detailed
        WHERE temperature IS NULL OR city IS NULL
    """)
    null_count = cursor.fetchone()[0]
    
    if null_count > 0:
        logger.warning(f"Valeurs nulles critiques: {null_count}")
    
    cursor.close()
    conn.close()
    
    logger.info("Vérification de la qualité des données terminée")
    
    # Pousser les métriques dans XCom
    context['ti'].xcom_push(key='total_records', value=count)
    context['ti'].xcom_push(key='duplicates', value=len(duplicates))
    context['ti'].xcom_push(key='null_records', value=null_count)


def send_success_notification(**context):
    """Envoie une notification de succès."""
    from loguru import logger
    
    ti = context['ti']
    
    # Récupérer les métriques depuis XCom
    ingestion_count = ti.xcom_pull(key='ingestion_count', task_ids='ingest_weather_data')
    total_records = ti.xcom_pull(key='total_records', task_ids='quality_check')
    
    message = f"""
    Pipeline exécuté avec succès!
    
    Exécution: {context['ds']}
    Données ingérées: {ingestion_count}
    Total d'enregistrements: {total_records}
    
    Tableau de bord disponible: http://localhost:8080
    """
    
    logger.info(message)
    
    # Ici, vous pouvez ajouter l'envoi d'email, Slack, etc.
    # from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


# Définition des tâches
with dag:
    
    # Tâche 1: Vérifier la disponibilité de l'API
    check_api = PythonOperator(
        task_id='check_api_availability',
        python_callable=check_api_availability,
        provide_context=True,
    )
    
    # Tâche 2: Ingérer les données vers Kafka
    ingest_data = PythonOperator(
        task_id='ingest_weather_data',
        python_callable=run_ingestion,
        provide_context=True,
    )
    
    # Tâche 3: Consumer Kafka et sauvegarder dans S3 (Bronze)
    consume_to_s3 = PythonOperator(
        task_id='consume_kafka_to_s3',
        python_callable=run_kafka_to_s3,
        provide_context=True,
    )
    
    # Tâche 4: Processing avec PySpark (Silver)
    process_silver = PythonOperator(
        task_id='process_silver_data',
        python_callable=run_silver_processing,
        provide_context=True,
    )
    
    # Tâche 5: Charger dans PostgreSQL (Gold)
    load_gold = PythonOperator(
        task_id='load_gold_data',
        python_callable=run_gold_loading,
        provide_context=True,
    )
    
    # Tâche 6: Vérification de la qualité des données
    quality_check = PythonOperator(
        task_id='quality_check',
        python_callable=data_quality_check,
        provide_context=True,
    )
    
    # Tâche 7: Notification de succès
    notify_success = PythonOperator(
        task_id='send_notification',
        python_callable=send_success_notification,
        provide_context=True,
        trigger_rule='all_success',
    )
    
    # Tâche 8: Nettoyage des anciennes données (optionnel, hebdomadaire)
    cleanup_old_data = BashOperator(
        task_id='cleanup_old_data',
        bash_command="""
        psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB -c \
        "SELECT * FROM cleanup_old_data(30);"
        """,
        trigger_rule='all_done',
    )
    
    # Définition des dépendances (ordre d'exécution)
    check_api >> ingest_data >> consume_to_s3 >> process_silver >> load_gold >> quality_check >> notify_success
    quality_check >> cleanup_old_data
