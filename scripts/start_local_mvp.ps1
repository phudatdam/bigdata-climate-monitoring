param(
    [string[]]$Locations = @("Hanoi"),
    [switch]$RunCollectors,
    [switch]$BuildProcessor
)

$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$logDirectory = Join-Path $repositoryRoot "logs\local-mvp"
$runId = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")

New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null

function ConvertTo-ProcessArgument {
    param([string]$Value)

    if ($Value -notmatch '[\s"]') {
        return $Value
    }

    $escaped = $Value -replace '(\\*)"', '$1$1\"'
    $escaped = $escaped -replace '(\\+)$', '$1$1'
    return '"' + $escaped + '"'
}

function Invoke-CheckedProcess {
    param(
        [string]$FilePath,
        [string[]]$Arguments,
        [string]$WorkingDirectory = $repositoryRoot,
        [int]$TimeoutSeconds = 60,
        [switch]$AllowFailure,
        [switch]$CaptureCmdExitCode
    )

    $name = [IO.Path]::GetFileNameWithoutExtension($FilePath)
    $commandId = [guid]::NewGuid().ToString("N").Substring(0, 8)
    $stdoutPath = Join-Path $logDirectory "$runId-$name-$commandId.stdout.log"
    $stderrPath = Join-Path $logDirectory "$runId-$name-$commandId.stderr.log"
    $argumentLine = (($Arguments | ForEach-Object { ConvertTo-ProcessArgument $_ }) -join " ")
    $launchFilePath = $FilePath
    $launchArguments = $argumentLine
    if ($CaptureCmdExitCode) {
        $resolvedPath = (Get-Command $FilePath -ErrorAction Stop).Source
        $commandLine = '"' + $resolvedPath + '" ' + $argumentLine +
            ' & echo __PROCESS_EXIT_CODE__%ERRORLEVEL%'
        $launchFilePath = $env:ComSpec
        $launchArguments = '/d /s /c "' + $commandLine + '"'
    }

    $process = Start-Process `
        -FilePath $launchFilePath `
        -ArgumentList $launchArguments `
        -WorkingDirectory $WorkingDirectory `
        -NoNewWindow `
        -PassThru `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath

    if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
        Stop-Process -Id $process.Id -Force
        throw "$name did not finish within $TimeoutSeconds seconds. See $stdoutPath and $stderrPath."
    }

    $stdout = ""
    if (Test-Path -LiteralPath $stdoutPath) {
        $stdoutContent = Get-Content -LiteralPath $stdoutPath -Raw
        if ($null -ne $stdoutContent) {
            $stdout = $stdoutContent.Trim()
        }
    }
    $stderr = ""
    if (Test-Path -LiteralPath $stderrPath) {
        $stderrContent = Get-Content -LiteralPath $stderrPath -Raw
        if ($null -ne $stderrContent) {
            $stderr = $stderrContent.Trim()
        }
    }

    $exitCode = $process.ExitCode
    if ($CaptureCmdExitCode) {
        if ($stdout -notmatch '(?m)^__PROCESS_EXIT_CODE__(\d+)$') {
            throw "$name did not report an exit status. See $stdoutPath and $stderrPath."
        }
        $exitCode = [int]$Matches[1]
        $stdout = ($stdout -replace '(?m)^__PROCESS_EXIT_CODE__\d+\s*$', "").Trim()
    }
    if ([string]::IsNullOrWhiteSpace([string]$exitCode)) {
        $exitCode = if ([string]::IsNullOrWhiteSpace($stderr)) { 0 } else { 1 }
    }
    if ($exitCode -ne 0 -and -not $AllowFailure) {
        throw "$name exited with code $exitCode.`n$stdout`n$stderr"
    }

    return [pscustomobject]@{
        ExitCode = $exitCode
        Stdout = $stdout
        Stderr = $stderr
    }
}

function Invoke-Docker {
    param(
        [string[]]$Arguments,
        [int]$TimeoutSeconds = 60
    )

    $result = Invoke-CheckedProcess `
        -FilePath "docker.exe" `
        -Arguments $Arguments `
        -TimeoutSeconds $TimeoutSeconds `
        -CaptureCmdExitCode
    return $result.Stdout.Trim()
}

function Test-HttpEndpoint {
    param(
        [string]$Uri,
        [int]$TimeoutSeconds = 5
    )

    try {
        $response = Invoke-WebRequest -Uri $Uri -TimeoutSec $TimeoutSeconds -UseBasicParsing
        return $response.StatusCode -eq 200
    } catch {
        return $false
    }
}

function Start-DevServerIfMissing {
    param(
        [string]$Name,
        [string]$Url,
        [string]$ProjectDirectory,
        [string]$NpmArguments
    )

    if (Test-HttpEndpoint -Uri $Url) {
        Write-Output "$Name is already responding at $Url"
        return
    }

    $packagePath = Join-Path (Join-Path $repositoryRoot $ProjectDirectory) "package.json"
    $modulesPath = Join-Path (Join-Path $repositoryRoot $ProjectDirectory) "node_modules"
    if (-not (Test-Path -LiteralPath $packagePath)) {
        throw "$Name package manifest is missing: $packagePath"
    }
    if (-not (Test-Path -LiteralPath $modulesPath)) {
        throw "$Name dependencies are not installed. Run npm --prefix $ProjectDirectory install first."
    }

    $stdoutPath = Join-Path $logDirectory "$runId-$ProjectDirectory.stdout.log"
    $stderrPath = Join-Path $logDirectory "$runId-$ProjectDirectory.stderr.log"
    Start-Process `
        -FilePath "npm.cmd" `
        -ArgumentList $NpmArguments `
        -WorkingDirectory (Join-Path $repositoryRoot $ProjectDirectory) `
        -PassThru `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath | Out-Null

    $deadline = (Get-Date).AddSeconds(45)
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 1
        if (Test-HttpEndpoint -Uri $Url) {
            Write-Output "$Name started at $Url"
            return
        }
    }

    throw "$Name did not become ready at $Url. See $stdoutPath and $stderrPath."
}

function Invoke-Collector {
    param([string]$CollectorPath)

    $result = Invoke-CheckedProcess `
        -FilePath "python.exe" `
        -Arguments (@($CollectorPath, "--run-once", "--locations") + $Locations) `
        -TimeoutSeconds 150

    Write-Output $result.Stdout.Trim()
    if ($result.Stderr) {
        Write-Output $result.Stderr.Trim()
    }

    $cycleEvents = @()
    foreach ($line in ($result.Stdout -split "`r?`n")) {
        if ($line.TrimStart().StartsWith("{")) {
            try {
                $event = $line | ConvertFrom-Json
                if ($event.event -like "*_cycle_completed") {
                    $cycleEvents += $event
                }
            } catch {
                # Non-event output is ignored; the collector's exit code remains authoritative.
            }
        }
    }

    if (-not $cycleEvents.Count) {
        throw "$CollectorPath finished without a cycle-completed event."
    }
    foreach ($event in $cycleEvents) {
        if ($event.failed_count -gt 0) {
            throw "$CollectorPath reported $($event.failed_count) failed record(s)."
        }
        if ($event.produced_count -eq 0) {
            Write-Warning "$CollectorPath produced no new records; the source may have returned an already-checkpointed event."
        }
    }
}

try {
    Set-Location $repositoryRoot

    $dockerVersion = Invoke-Docker -Arguments @("version", "--format", "{{.Server.Version}}") -TimeoutSeconds 15
    if (-not $dockerVersion) {
        throw "Docker Engine did not return a server version."
    }
    Write-Output "Docker Engine $dockerVersion"

    Invoke-Docker -Arguments @(
        "compose", "-f", "Ingestion\docker-compose.yml", "up", "-d", "kafka"
    ) -TimeoutSeconds 120 | Out-Null
    Invoke-Docker -Arguments @(
        "compose", "-f", "database\docker-compose.mongo.yml", "up", "-d"
    ) -TimeoutSeconds 120 | Out-Null

    $kafkaContainerId = Invoke-Docker -Arguments @(
        "compose", "-f", "Ingestion\docker-compose.yml", "ps", "-q", "kafka"
    ) -TimeoutSeconds 30
    if (-not $kafkaContainerId) {
        throw "Kafka container was not created by Compose."
    }

    $kafkaHealth = "starting"
    $healthDeadline = (Get-Date).AddSeconds(60)
    while ((Get-Date) -lt $healthDeadline) {
        $kafkaHealth = Invoke-Docker -Arguments @(
            "inspect", "--format", "{{if .State.Health}}{{.State.Health.Status}}{{else}}not-configured{{end}}",
            $kafkaContainerId
        ) -TimeoutSeconds 15
        if ($kafkaHealth -eq "healthy") {
            break
        }
        if ($kafkaHealth -eq "unhealthy") {
            break
        }
        Start-Sleep -Seconds 5
    }
    if ($kafkaHealth -ne "healthy") {
        throw "Kafka container health is '$kafkaHealth'. The helper will not restart an existing unhealthy broker; check with docker compose -f Ingestion\docker-compose.yml logs --tail 100 kafka."
    }

    $processorState = Invoke-Docker -Arguments @(
        "compose", "-f", "processing\docker-compose.yaml",
        "ps", "--status", "running", "--services"
    ) -TimeoutSeconds 30
    if ($processorState -notmatch "(?m)^processor$") {
        $processorArguments = @(
            "compose", "-f", "processing\docker-compose.yaml", "up", "-d"
        )
        if ($BuildProcessor) {
            Write-Output "Building and starting the Spark processor..."
            $processorArguments += "--build"
        } else {
            Write-Output "Starting the Spark processor from its existing image..."
            $processorArguments += "--no-build"
        }
        $processorArguments += "processor"
        $processorTimeout = if ($BuildProcessor) { 900 } else { 120 }
        Invoke-Docker -Arguments $processorArguments -TimeoutSeconds $processorTimeout | Out-Null
    } else {
        Write-Output "Spark processor is already running."
    }

    Invoke-CheckedProcess `
        -FilePath "python.exe" `
        -Arguments @("Ingestion\scripts\create_kafka_topics.py") `
        -TimeoutSeconds 60 | Out-Null

    Start-DevServerIfMissing `
        -Name "Backend API" `
        -Url "http://localhost:3001/api/health" `
        -ProjectDirectory "backend" `
        -NpmArguments "run dev"
    Start-DevServerIfMissing `
        -Name "Frontend" `
        -Url "http://localhost:5173" `
        -ProjectDirectory "frontend" `
        -NpmArguments "run dev -- --host 127.0.0.1"

    $locationId = ($Locations[0] -replace "[^A-Za-z0-9]", "").ToLowerInvariant()
    $weatherUrl = "http://localhost:3001/api/weather/hourly/$locationId"
    $airUrl = "http://localhost:3001/api/air-quality/$locationId"
    foreach ($url in @($weatherUrl, $airUrl)) {
        $response = Invoke-WebRequest -Uri $url -TimeoutSec 15 -UseBasicParsing
        if ($response.StatusCode -ne 200) {
            throw "API data check failed with HTTP $($response.StatusCode): $url"
        }
        Write-Output "API data route responded HTTP $($response.StatusCode): $url"
    }

    if ($RunCollectors) {
        Invoke-Collector -CollectorPath "Ingestion\collectors\weather_stream_collector.py"
        Invoke-Collector -CollectorPath "Ingestion\collectors\aqicn_air_stream_collector.py"
    }

    Write-Output "Local MVP startup/check completed. Logs: $logDirectory"
} catch {
    Write-Error $_
    exit 1
}
