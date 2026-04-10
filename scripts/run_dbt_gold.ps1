# Agora Terminal - dbt Gold runner
# Called by Windows Task Scheduler daily at 1:00 PM IST
# Order: cleanup orphans -> dedup silver -> snapshot -> gold

$projectRoot = "C:\Projects\agora-terminal\agora-terminal"
$logPath = "$projectRoot\scripts\dbt_gold.log"
$maxLogBytes = 5MB

# Log rotation
if ((Test-Path $logPath) -and (Get-Item $logPath).Length -gt $maxLogBytes) {
    Move-Item $logPath "$logPath.bak" -Force
}

function Log($msg) {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$ts] $msg"
    Write-Host $line
    Add-Content $logPath $line
}

function Die($msg) {
    Log "ERROR: $msg"
    Log "Pipeline aborted."
    exit 1
}

Log "===== dbt Gold pipeline starting ====="

# Step 0 - Backup Bronze JSONL to MinIO
Log "Step 0: Backing up Bronze JSONL to MinIO..."
$result = & python "$projectRoot\scripts\backup_bronze.py" 2>&1
Log "  $result"
if ($LASTEXITCODE -ne 0) {
    Log "WARNING: Bronze backup failed -- continuing pipeline"
}

# Step 1 - Clean up orphan docker-dbt-run-* containers
Log "Step 1: Cleaning orphan dbt containers..."
$orphans = docker ps -a --filter "name=docker-dbt-run" --format "{{.Names}}" 2>$null
if ($orphans) {
    $orphans | ForEach-Object {
        docker rm -f $_ | Out-Null
        Log "  Removed: $_"
    }
} else {
    Log "  No orphans found."
}

# Step 2 - Deduplicate Silver
Log "Step 2: Deduplicating silver_equity_ohlcv_daily..."
$dedupScript = "$projectRoot\scripts\dedup_silver.py"
if (-not (Test-Path $dedupScript)) {
    Die "dedup_silver.py not found at $dedupScript"
}
$result = & python $dedupScript 2>&1
Log "  $result"
if ($LASTEXITCODE -ne 0) {
    Die "Silver dedup failed (exit $LASTEXITCODE)"
}

# Step 3 - dbt snapshot
Log "Step 3: Running dbt snapshot..."
Set-Location $projectRoot
& C:\dbt-env\Scripts\Activate.ps1
$result = & dbt snapshot `
    --profiles-dir "$projectRoot\transform\dbt" `
    --project-dir "$projectRoot\transform\dbt\agora" `
    2>&1
Add-Content $logPath ($result | Out-String)
if ($LASTEXITCODE -ne 0) {
    Die "dbt snapshot failed (exit $LASTEXITCODE)"
}
Log "Step 3: dbt snapshot OK"

# Step 4 - dbt Gold
Log "Step 4: Running dbt Gold..."
$result = & dbt run `
    --select tag:gold --full-refresh `
    --profiles-dir "$projectRoot\transform\dbt" `
    --project-dir "$projectRoot\transform\dbt\agora" `
    2>&1
Add-Content $logPath ($result | Out-String)
if ($LASTEXITCODE -ne 0) {
    Die "dbt Gold failed (exit $LASTEXITCODE)"
}
Log "Step 4: dbt Gold OK"

Log "===== dbt Gold pipeline complete ====="
exit 0