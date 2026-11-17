param(
    [Parameter(Mandatory = $true)]
    [string]$StartDate,
    [Parameter(Mandatory = $true)]
    [string]$EndDate,
    [string[]]$Locations = @("Hanoi"),
    [switch]$AllLocations,
    [switch]$BuildProcessor
)

$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$pythonVenv = Join-Path $repositoryRoot ".venv\Scripts\python.exe"
$pythonExecutable = if (Test-Path -LiteralPath $pythonVenv) {
    $pythonVenv
} else {
    (Get-Command python.exe -ErrorAction Stop).Source
}
$runId = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
$logDirectory = Join-Path $repositoryRoot "logs\backfill"
$collectorLog = Join-Path $logDirectory "weather_backfill_$runId.log"
$historicalScript = Join-Path $repositoryRoot "scripts\run_historical_batch.ps1"

foreach ($dateText in @($StartDate, $EndDate)) {
    $parsedDate = [datetime]::MinValue
    $valid = [datetime]::TryParseExact(
        $dateText,
        "yyyy-MM-dd",
        [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::None,
        [ref]$parsedDate
    )
    if (-not $valid) {
        throw "Invalid date '$dateText'; expected YYYY-MM-DD."
    }
}
$start = [datetime]::ParseExact($StartDate, "yyyy-MM-dd", [Globalization.CultureInfo]::InvariantCulture)
$end = [datetime]::ParseExact($EndDate, "yyyy-MM-dd", [Globalization.CultureInfo]::InvariantCulture)
$dayCount = ($end.Date - $start.Date).Days + 1
if ($dayCount -lt 1 -or $dayCount -gt 31) {
    throw "Backfill range must be ordered and no longer than 31 calendar days."
}
if ($AllLocations -and $Locations.Count -gt 1) {
    throw "Use either -AllLocations or -Locations, not both."
}

New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null
Set-Location $repositoryRoot

Write-Output "Creating configured Kafka topics, including the isolated weather backfill topic."
& $pythonExecutable "Ingestion\scripts\create_kafka_topics.py"
if ($LASTEXITCODE -ne 0) {
    throw "Kafka topic setup failed with exit code $LASTEXITCODE."
}

$collectorArguments = @(
    "Ingestion\collectors\weather_batch_collector.py",
    "--start-date",
    $StartDate,
    "--end-date",
    $EndDate,
    "--topic",
    "weather.raw.batch"
)
if (-not $AllLocations) {
    $collectorArguments += "--locations"
    $collectorArguments += $Locations
}

$scope = if ($AllLocations) { "all configured cities" } else { $Locations -join ", " }
Write-Output "Fetching $dayCount UTC calendar days for $scope into weather.raw.batch."
& $pythonExecutable @collectorArguments *> $collectorLog
if ($LASTEXITCODE -ne 0) {
    Get-Content -LiteralPath $collectorLog -Tail 60
    throw "Weather archive collection failed with exit code $LASTEXITCODE. See $collectorLog."
}
Select-String -LiteralPath $collectorLog -Pattern "weather_batch_started|weather_batch_completed" |
    ForEach-Object { Write-Output $_.Line }

$topics = @("weather.raw.batch", "weather.raw.stream", "air_quality.raw.stream")
$historicalArguments = @(
    "-NoProfile",
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    "`"$historicalScript`"",
    "-Topics",
    ($topics -join ",")
)
if ($BuildProcessor) {
    $historicalArguments += "-BuildProcessor"
}

Write-Output "Exporting the backfill together with retained live topics and applying the historical batch."
$childPowerShell = Join-Path $PSHOME "powershell.exe"
$historicalProcess = Start-Process `
    -FilePath $childPowerShell `
    -ArgumentList ($historicalArguments -join " ") `
    -WorkingDirectory $repositoryRoot `
    -NoNewWindow `
    -Wait `
    -PassThru
if ($historicalProcess.ExitCode -ne 0) {
    throw "Historical Spark batch failed with exit code $($historicalProcess.ExitCode)."
}

Write-Output "Weather backfill completed. Collector log: $collectorLog"
