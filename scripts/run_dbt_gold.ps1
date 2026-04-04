# Agora Terminal — dbt Gold runner
# Called by Windows Task Scheduler after equity_daily_schedule completes
$logPath = "C:\Projects\agora-terminal\agora-terminal\scripts\dbt_gold.log"
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
Add-Content $logPath "[$timestamp] Starting dbt Gold run..."

try {
    & C:\dbt-env\Scripts\activate.ps1
    $result = & dbt run --select tag:gold --profiles-dir transform\dbt --project-dir transform\dbt\agora 2>&1
    Add-Content $logPath $result
    Add-Content $logPath "[$timestamp] dbt Gold run complete."
} catch {
    Add-Content $logPath "[$timestamp] ERROR: $_"
}