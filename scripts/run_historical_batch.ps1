param(
    [string[]]$Topics = @("weather.raw.stream", "air_quality.raw.stream"),
    [switch]$DryRun,
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
$logDirectory = Join-Path $repositoryRoot "logs"
$runId = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
$logPath = Join-Path $logDirectory "historical_batch_$runId.log"

New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null
Start-Transcript -Path $logPath -Force | Out-Null

try {
    Set-Location $repositoryRoot
    if (-not $env:SPARK_SQL_SHUFFLE_PARTITIONS) {
        $env:SPARK_SQL_SHUFFLE_PARTITIONS = "8"
    }

    $normalizedTopics = @()
    foreach ($t in $Topics) {
        if ($t -match ",") {
            $normalizedTopics += ($t -split ",") | ForEach-Object { $_.Trim() } | Where-Object { $_ }
        } else {
            $normalizedTopics += $t
        }
    }

    $exportArguments = @(
        "Ingestion\scripts\export_kafka_to_file.py",
        "--topics"
    ) + $normalizedTopics + @(
        "--from-beginning",
        "--max-messages",
        "0",
        "--include-kafka-meta"
    )
    $exportOutput = & $pythonExecutable @exportArguments
    if ($LASTEXITCODE -ne 0) {
        throw "Kafka export failed with exit code $LASTEXITCODE"
    }

    $exportResult = ($exportOutput -join "`n") | ConvertFrom-Json
    $manifestPath = [string]$exportResult.manifest
    if (-not $manifestPath -or -not (Test-Path -LiteralPath $manifestPath)) {
        throw "Kafka exporter did not create its manifest"
    }

    $manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
    if ($manifest.complete -ne $true) {
        throw "Kafka export is incomplete: $($manifest.completion_reason)"
    }

    $inputInContainer = "/workspace/exports/$($exportResult.output | Split-Path -Leaf)"
    $manifestInContainer = "/workspace/exports/$($manifestPath | Split-Path -Leaf)"
    $batchImageId = & docker image inspect "processing-historical-batch:latest" --format "{{.Id}}" 2>$null
    $batchImageAvailable = ($LASTEXITCODE -eq 0 -and $batchImageId)
    $batchArguments = @(
        "compose",
        "-f",
        "processing\docker-compose.yaml",
        "run",
        "--rm",
        "--no-deps"
    )
    if ($BuildProcessor -or -not $batchImageAvailable) {
        $batchArguments += "--build"
    }
    $batchArguments += @(
        "historical-batch",
        "--input",
        $inputInContainer,
        "--manifest",
        $manifestInContainer,
        "--parquet-archive-root",
        "/workspace/archive"
    )
    if ($DryRun) {
        $batchArguments += "--dry-run"
    }

    & docker @batchArguments
    if ($LASTEXITCODE -ne 0) {
        throw "Historical Spark batch failed with exit code $LASTEXITCODE"
    }

    Write-Output "Historical batch completed. Raw export: $($exportResult.output); Parquet snapshots: $($repositoryRoot)\archive"
}
catch {
    Write-Error $_
    exit 1
}
finally {
    Stop-Transcript | Out-Null
}
