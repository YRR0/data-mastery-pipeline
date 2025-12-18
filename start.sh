#!/bin/bash

# Script de démarrage du pipeline de données météo
# Ce script initialise tous les services et lance le pipeline

set -e  # Arrêter en cas d'erreur

echo "=========================================="
echo "DÉMARRAGE DU PIPELINE DE DONNÉES MÉTÉO"
echo "=========================================="

# Vérifier que Docker est installé
if ! command -v docker &> /dev/null; then
    echo "❌ Docker n'est pas installé"
    exit 1
fi

echo "✓ Docker détecté"

# Vérifier que docker-compose est installé
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose n'est pas installé"
    exit 1
fi

echo "✓ Docker Compose détecté"

# Vérifier que le fichier .env existe
if [ ! -f .env ]; then
    echo "⚠️  Fichier .env non trouvé, création depuis .env.example"
    cp .env.example .env
    echo "⚠️  ATTENTION: Pensez à configurer votre clé API OpenWeather dans .env"
    exit 1
fi

echo "✓ Fichier .env trouvé"

# Créer les répertoires nécessaires
echo ""
echo "Création des répertoires..."
mkdir -p logs
mkdir -p airflow/logs
mkdir -p airflow/plugins

# Définir l'UID pour Airflow
export AIRFLOW_UID=$(id -u)

echo "✓ Répertoires créés"

# Arrêter les conteneurs existants
echo ""
echo "Arrêt des conteneurs existants..."
docker-compose down

# Construire les images
echo ""
echo "Construction des images Docker..."
docker-compose build

# Démarrer les services
echo ""
echo "Démarrage des services..."
docker-compose up -d

# Attendre que les services soient prêts
echo ""
echo "Attente du démarrage des services (60 secondes)..."
sleep 60

# Vérifier l'état des services
echo ""
echo "Vérification de l'état des services..."
docker-compose ps

# Afficher les URLs d'accès
echo ""
echo "=========================================="
echo "SERVICES DISPONIBLES"
echo "=========================================="
echo "Airflow UI:       http://localhost:8080"
echo "  Username:       admin"
echo "  Password:       admin"
echo ""
echo "MinIO Console:    http://localhost:9001"
echo "  Username:       minioadmin"
echo "  Password:       minioadmin"
echo ""
echo "Kafka UI:         http://localhost:9021"
echo ""
echo "Spark UI:         http://localhost:8081"
echo ""
echo "PostgreSQL:       localhost:5432"
echo "  Database:       weather_dw"
echo "  Username:       dataeng"
echo "  Password:       dataeng123"
echo "=========================================="

echo ""
echo "✓ Pipeline démarré avec succès!"
echo ""
echo "Pour voir les logs:"
echo "  docker-compose logs -f [service_name]"
echo ""
echo "Pour arrêter le pipeline:"
echo "  docker-compose down"
echo ""
