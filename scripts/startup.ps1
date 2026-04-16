# Agora Terminal - Startup Script
# Run after laptop wake or reboot
# Usage: .\scripts\startup.ps1

$projectRoot = "C:\Projects\agora-terminal\agora-terminal"

Write-Host "Starting Agora Terminal..." -ForegroundColor Cyan

# Step 1 - Bring up containers
Write-Host "[1/5] Starting containers..." -ForegroundColor Yellow
Set-Location $projectRoot
docker compose -f infra/docker/docker-compose.yml up -d
Start-Sleep -Seconds 5

# Step 2 - Start dashboard server
Write-Host "[2/5] Starting dashboard server..." -ForegroundColor Yellow
Start-Job -ScriptBlock { 
    Set-Location "C:\Projects\agora-terminal\agora-terminal\dashboard\src\modules"
    python -m http.server 8080 
} | Out-Null

# Step 3 - Stop API to release DuckDB lock
Write-Host "[3/5] Releasing DuckDB lock..." -ForegroundColor Yellow
docker stop agora-api | Out-Null

# Step 4 - Run dbt Gold with fct_prices full-refresh
Write-Host "[4/5] Running dbt Gold..." -ForegroundColor Yellow
& C:\dbt-env\Scripts\Activate.ps1
& dbt run --select fct_prices --full-refresh --profiles-dir "$projectRoot\transform\dbt" --project-dir "$projectRoot\transform\dbt\agora" | Out-Null
& dbt run --exclude fct_prices --profiles-dir "$projectRoot\transform\dbt" --project-dir "$projectRoot\transform\dbt\agora" | Out-Null

# Step 5 - Restart API
Write-Host "[5/5] Starting API..." -ForegroundColor Yellow
docker compose -f infra/docker/docker-compose.yml up -d --no-deps --force-recreate api

Write-Host ""
Write-Host "Done! Open: http://localhost:8080/market_overview/index.html" -ForegroundColor Green