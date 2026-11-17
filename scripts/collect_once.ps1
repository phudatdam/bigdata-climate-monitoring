param(
    [string[]]$Locations = @(),
    [string]$PythonExecutable = ""
)

$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$logDirectory = Join-Path $repositoryRoot "logs\collection"
$runId = "{0}_{1}" -f (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss"), ([guid]::NewGuid().ToString("N").Substring(0, 8))
$transcriptPath = Join-Path $logDirectory "collection_$runId.log"
$summaryPath = Join-Path $logDirectory "collection_$runId.summary.json"
$lockPath = Join-Path $logDirectory "collector.lock"
$runStartedAt = (Get-Date).ToUniversalTime().ToString("o")
$lockStream = $null
$transcriptStarted = $false
$summary = [ordered]@{
    run_id = $runId
    started_at = $runStartedAt
    requested_locations = @($Locations)
    collectors = @()
    status = "running"
}

New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null

function Resolve-PythonExecutable {
    if ($PythonExecutable) {
        $resolved = (Resolve-Path -LiteralPath $PythonExecutable -ErrorAction Stop).Path
        return $resolved
    }

    $venvPython = Join-Path $repositoryRoot ".venv\Scripts\python.exe"
    if (Test-Path -LiteralPath $venvPython) {
        return $venvPython
    }

    $pythonCommand = Get-Command python.exe -ErrorAction Stop
    return $pythonCommand.Source
}

function Invoke-CollectorOnce {
    param(
        [string]$Name,
        [string]$ScriptPath,
        [string]$EventPrefix
    )

    $arguments = @($ScriptPath, "--run-once")
    if ($Locations.Count -gt 0) {
        $arguments += "--locations"
        $arguments += $Locations
    }

    Write-Host "Starting $Name collector."
    $lines = @(& $script:pythonExecutable @arguments 2>&1)
    $exitCode = $LASTEXITCODE
    $cycleEvents = @()
    $startEvents = @()
    $observationEvents = @()
    foreach ($lineObject in $lines) {
        $line = [string]$lineObject
        Write-Host $line
        if ($line.TrimStart().StartsWith("{")) {
            try {
                $event = $line | ConvertFrom-Json
                if ($event.event -like "$EventPrefix`*_cycle_completed") {
                    $cycleEvents += $event
                } elseif ($event.event -like "$EventPrefix`*_started") {
                    $startEvents += $event
                } elseif (
                    $event.event -like "$EventPrefix`*_record_produced" -or
                    $event.event -like "$EventPrefix`*_skipped_duplicate"
                ) {
                    $observationEvents += [pscustomobject]@{
                        status = if ($event.event -like "*_record_produced") { "produced" } else { "skipped" }
                        city = $event.city
                        event_time = $event.event_time
                        source = $event.source
                        partition = $event.partition
                        offset = $event.offset
                    }
                }
            } catch {
                # Collector diagnostics remain in the transcript; only valid JSON events enter the summary.
            }
        }
    }

    if ($exitCode -ne 0) {
        throw "$Name collector exited with code $exitCode."
    }
    if ($cycleEvents.Count -eq 0) {
        throw "$Name collector completed without a cycle-completed event."
    }

    $produced = 0
    $skipped = 0
    $failed = 0
    foreach ($cycle in $cycleEvents) {
        $produced += [int]$cycle.produced_count
        $skipped += [int]$cycle.skipped_count
        $failed += [int]$cycle.failed_count
    }
    if ($failed -gt 0) {
        throw "$Name collector reported $failed failed record(s); inspect the transcript and DLQ."
    }
    $eventTimes = @(
        $observationEvents |
            Where-Object { $_.event_time } |
            Sort-Object event_time
    )

    return [pscustomobject]@{
        collector = $Name
        topic = $cycleEvents[0].topic
        poll_started_at = if ($startEvents.Count -gt 0) { $startEvents[0].started_at } else { $null }
        poll_finished_at = $cycleEvents[-1].finished_at
        last_successful_event_time = if ($eventTimes.Count -gt 0) { $eventTimes[-1].event_time } else { $null }
        produced_count = $produced
        skipped_count = $skipped
        failed_count = $failed
        records = @($observationEvents)
    }
}

try {
    try {
        $lockStream = [System.IO.File]::Open(
            $lockPath,
            [System.IO.FileMode]::OpenOrCreate,
            [System.IO.FileAccess]::ReadWrite,
            [System.IO.FileShare]::None
        )
    } catch [System.IO.IOException] {
        Write-Warning "A collection run is already active; skipping this overlapping invocation."
        exit 0
    }

    Start-Transcript -Path $transcriptPath -Force | Out-Null
    $transcriptStarted = $true
    Set-Location $repositoryRoot
    $script:pythonExecutable = Resolve-PythonExecutable

    $summary.collectors += Invoke-CollectorOnce `
        -Name "weather" `
        -ScriptPath "Ingestion\collectors\weather_stream_collector.py" `
        -EventPrefix "weather_stream"
    $summary.collectors += Invoke-CollectorOnce `
        -Name "air_quality" `
        -ScriptPath "Ingestion\collectors\aqicn_air_stream_collector.py" `
        -EventPrefix "aqicn_air"
    $summary.status = "succeeded"
    $summary.finished_at = (Get-Date).ToUniversalTime().ToString("o")
    $summary | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $summaryPath -Encoding UTF8
    Write-Output ($summary | ConvertTo-Json -Depth 8)
    Write-Output "Collection summary: $summaryPath"
} catch {
    $failure = $_
    $summary.status = "failed"
    $summary.finished_at = (Get-Date).ToUniversalTime().ToString("o")
    $summary.error = $failure.Exception.Message
    $summary.transcript = if ($transcriptStarted) { $transcriptPath } else { $null }
    $summary | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $summaryPath -Encoding UTF8
    Write-Error $failure
    exit 1
} finally {
    if ($transcriptStarted) {
        Stop-Transcript | Out-Null
    }
    if ($lockStream) {
        $lockStream.Dispose()
    }
}
