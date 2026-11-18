param(
    [ValidateRange(1, 31)]
    [int]$LookbackDays = 7,
    [switch]$BuildProcessor
)

$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$utcToday = [datetime]::UtcNow.Date
$startDate = $utcToday.AddDays(-($LookbackDays - 1)).ToString("yyyy-MM-dd")
$endDate = $utcToday.AddDays(-1).ToString("yyyy-MM-dd")
$backfillScript = Join-Path $PSScriptRoot "run_weather_backfill.ps1"
$arguments = @(
    "-StartDate", $startDate,
    "-EndDate", $endDate,
    "-AllLocations"
)
if ($BuildProcessor) {
    $arguments += "-BuildProcessor"
}
& $backfillScript @arguments

if ($LASTEXITCODE -ne 0) {
    throw "Incremental climate backfill failed."
}
