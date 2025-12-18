# Script PowerShell de démarrage du pipeline de données météo
# Ce script initialise tous les services et lance le pipeline

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "DÉMARRAGE DU PIPELINE DE DONNÉES MÉTÉO" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

# Vérifier que Docker est installé
try {
    docker --version | Out-Null
    Write-Host "✓ Docker détecté" -ForegroundColor Green
} catch {
    Write-Host "❌ Docker n'est pas installé" -ForegroundColor Red
    exit 1
}

# Vérifier que docker-compose est installé
try {
    docker-compose --version | Out-Null
    Write-Host "✓ Docker Compose détecté" -ForegroundColor Green
} catch {
    Write-Host "❌ Docker Compose n'est pas installé" -ForegroundColor Red
    exit 1
}

# Vérifier que le fichier .env existe
if (-not (Test-Path .env)) {
    Write-Host "⚠️  Fichier .env non trouvé, création depuis .env.example" -ForegroundColor Yellow
    Copy-Item .env.example .env
    Write-Host "⚠️  ATTENTION: Pensez à configurer votre clé API OpenWeather dans .env" -ForegroundColor Yellow
    exit 1
}

Write-Host "✓ Fichier .env trouvé" -ForegroundColor Green

# Créer les répertoires nécessaires
Write-Host ""
Write-Host "Création des répertoires..." -ForegroundColor Cyan
New-Item -ItemType Directory -Force -Path logs | Out-Null
New-Item -ItemType Directory -Force -Path airflow\logs | Out-Null
New-Item -ItemType Directory -Force -Path airflow\plugins | Out-Null

Write-Host "✓ Répertoires créés" -ForegroundColor Green

# Définir l'UID pour Airflow (Windows utilise 50000 par défaut)
$env:AIRFLOW_UID = 50000

# Arrêter les conteneurs existants
Write-Host ""
Write-Host "Arrêt des conteneurs existants..." -ForegroundColor Cyan
docker-compose down

# Construire les images
Write-Host ""
Write-Host "Construction des images Docker..." -ForegroundColor Cyan
docker-compose build

# Démarrer les services
Write-Host ""
Write-Host "Démarrage des services..." -ForegroundColor Cyan
docker-compose up -d

# Attendre que les services soient prêts
Write-Host ""
Write-Host "Attente du démarrage des services (60 secondes)..." -ForegroundColor Cyan
Start-Sleep -Seconds 60

# Vérifier l'état des services
Write-Host ""
Write-Host "Vérification de l'état des services..." -ForegroundColor Cyan
docker-compose ps

# Afficher les URLs d'accès
Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "SERVICES DISPONIBLES" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Airflow UI:       http://localhost:8080" -ForegroundColor White
Write-Host "  Username:       admin" -ForegroundColor Gray
Write-Host "  Password:       admin" -ForegroundColor Gray
Write-Host ""
Write-Host "MinIO Console:    http://localhost:9001" -ForegroundColor White
Write-Host "  Username:       minioadmin" -ForegroundColor Gray
Write-Host "  Password:       minioadmin" -ForegroundColor Gray
Write-Host ""
Write-Host "Kafka UI:         http://localhost:9021" -ForegroundColor White
Write-Host ""
Write-Host "Spark UI:         http://localhost:8081" -ForegroundColor White
Write-Host ""
Write-Host "PostgreSQL:       localhost:5432" -ForegroundColor White
Write-Host "  Database:       weather_dw" -ForegroundColor Gray
Write-Host "  Username:       dataeng" -ForegroundColor Gray
Write-Host "  Password:       dataeng123" -ForegroundColor Gray
Write-Host "==========================================" -ForegroundColor Cyan

Write-Host ""
Write-Host "✓ Pipeline démarré avec succès!" -ForegroundColor Green
Write-Host ""
Write-Host "Pour voir les logs:" -ForegroundColor Yellow
Write-Host "  docker-compose logs -f [service_name]" -ForegroundColor White
Write-Host ""
Write-Host "Pour arrêter le pipeline:" -ForegroundColor Yellow
Write-Host "  docker-compose down" -ForegroundColor White
Write-Host ""
