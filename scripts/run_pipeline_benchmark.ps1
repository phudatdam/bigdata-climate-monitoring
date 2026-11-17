param(
    [ValidateRange(2, 10000000)]
    [int]$Records = 100,
    [ValidateRange(1, 10000000)]
    [int]$KafkaRecords = 1000,
    [ValidateRange(1, 12)]
    [int]$KafkaPartitions = 3,
    [string]$ApiUrl = "http://host.docker.internal:3001",
    [string]$ApiDate = "",
    [switch]$SkipKafka,
    [switch]$SkipMongo,
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
$exportDirectory = Join-Path $repositoryRoot "exports\benchmark"
$reportDirectory = Join-Path $repositoryRoot "logs\benchmarks"
$inputPath = Join-Path $exportDirectory "synthetic_snapshot_$Records.jsonl"
$manifestPath = "$inputPath.manifest.json"
$inputInContainer = "/workspace/exports/benchmark/synthetic_snapshot_$Records.jsonl"
$manifestInContainer = "$inputInContainer.manifest.json"

New-Item -ItemType Directory -Path $exportDirectory -Force | Out-Null
New-Item -ItemType Directory -Path $reportDirectory -Force | Out-Null
Set-Location $repositoryRoot

Write-Output "Generating $Records deterministic synthetic Kafka-shaped records."
& $pythonExecutable -m processing.benchmarks.generate_synthetic_snapshot `
    --records $Records `
    --output-dir $exportDirectory
if ($LASTEXITCODE -ne 0) {
    throw "Synthetic snapshot generation failed with exit code $LASTEXITCODE."
}
if (-not (Test-Path -LiteralPath $inputPath) -or -not (Test-Path -LiteralPath $manifestPath)) {
    throw "Synthetic input or manifest was not created."
}

if (-not $SkipKafka) {
    Write-Output "Measuring Kafka produce/consume with $KafkaRecords records in a disposable topic."
    & $pythonExecutable -m processing.benchmarks.benchmark_kafka `
        --records $KafkaRecords `
        --partitions $KafkaPartitions
    if ($LASTEXITCODE -ne 0) {
        throw "Kafka benchmark failed with exit code $LASTEXITCODE."
    }
}

$batchArguments = @(
    "compose",
    "-f",
    "processing\docker-compose.yaml",
    "run",
    "--rm",
    "--no-deps"
)
$batchImageId = & docker image inspect "processing-historical-batch:latest" --format "{{.Id}}" 2>$null
$batchImageAvailable = ($LASTEXITCODE -eq 0 -and $batchImageId)
if ($BuildProcessor -or -not $batchImageAvailable) {
    $batchArguments += "--build"
}
$batchArguments += @(
    "--entrypoint",
    "python",
    "historical-batch",
    "-m",
    "processing.benchmarks.run_spark_benchmark",
    "--input",
    $inputInContainer,
    "--manifest",
    $manifestInContainer,
    "--archive-root",
    "/workspace/archive/benchmark_runs",
    "--report-dir",
    "/workspace/benchmark-reports",
    "--api-url",
    $ApiUrl
)
if ($ApiDate) {
    $batchArguments += @("--api-date", $ApiDate)
}
if (-not $SkipMongo) {
    $batchArguments += "--write-mongodb"
}

Write-Output "Running Spark, Parquet, optional MongoDB, and API measurements."
& docker @batchArguments
if ($LASTEXITCODE -ne 0) {
    throw "Spark benchmark failed with exit code $LASTEXITCODE."
}

$report = Get-ChildItem -LiteralPath $reportDirectory -Filter "spark_benchmark_${Records}_*.json" |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1
if (-not $report) {
    throw "Spark benchmark completed without a report under $reportDirectory."
}
Write-Output "Benchmark report: $($report.FullName)"
Get-Content -LiteralPath $report.FullName -Raw
