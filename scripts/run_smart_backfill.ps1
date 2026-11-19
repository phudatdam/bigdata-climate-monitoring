param(
    [string[]]$Locations = @("Hanoi"),
    [switch]$AllLocations,
    [ValidateRange(1, 31)]
    [int]$LookbackDays = 7,
    [switch]$DryRun,
    [switch]$BuildProcessor
)

$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$backendDir = Join-Path $repositoryRoot "backend"
$detectorScript = Join-Path $backendDir "dist\smartGapDetector.js"
$backfillScript = Join-Path $PSScriptRoot "run_weather_backfill.ps1"

Write-Output "============================================================"
Write-Output "    Smart Data Gap Detection & Targeted Backfill Runner    "
Write-Output "============================================================"
Write-Output "Scanning past $LookbackDays day(s) for intermittent missing records and temporal gaps..."

# Ensure detector script is compiled
if (-not (Test-Path -LiteralPath $detectorScript)) {
    Write-Output "Compiling backend smartGapDetector..."
    npm --prefix $backendDir run build | Out-Null
}

$detectorArgs = @($detectorScript, "--lookback", $LookbackDays)
if ($AllLocations) {
    $detectorArgs += "--all-locations"
} else {
    $detectorArgs += @("--locations", ($Locations -join ","))
}

$nodeExecutable = (Get-Command node.exe -ErrorAction Stop).Source
$jsonOutput = & $nodeExecutable @detectorArgs
if ($LASTEXITCODE -ne 0) {
    throw "Gap detection script failed with exit code $LASTEXITCODE.`n$jsonOutput"
}

$result = $jsonOutput | ConvertFrom-Json
$jobs = $result.jobs

Write-Output "`n[SCAN RESULTS]"
Write-Output "  Lookback Period:   $($result.lookbackDays) days"
Write-Output "  Scanned Window:    $($result.scanStartIso) to $($result.scanEndIso)"
Write-Output "  Locations Scanned: $($result.locationsScanned -join ', ')"
Write-Output "  Total Gaps Found:  $($result.detectedGapsCount)"
Write-Output "  Backfill Jobs:     $($jobs.Count)"

if ($jobs.Count -eq 0) {
    Write-Output "`n[OK] No missing observation gaps detected! All historical readings are complete."
    Write-Output "     Skipping backfill execution to preserve API quotas and workstation resources."
    exit 0
}

Write-Output "`n[DETECTED TARGETED GAPS]:"
foreach ($job in $jobs) {
    $streamsText = $job.streams -join ", "
    $reasonsText = $job.reasons -join "; "
    Write-Output "  * Location: $($job.locationKey) | Range: $($job.startDate) -> $($job.endDate) (~$($job.missingHours)h missing)"
    Write-Output "    Streams:  $streamsText"
    Write-Output "    Reasons:  $reasonsText"
    Write-Output "    Command:  $($job.command)"
}

if ($DryRun) {
    Write-Output "`n[DRY RUN PREVIEW] - No backfills were executed."
    Write-Output "To execute these backfill jobs, run without -DryRun."
    exit 0
}

Write-Output "`nExecuting $($jobs.Count) targeted backfill job(s)..."
$jobIndex = 1
foreach ($job in $jobs) {
    Write-Output "`n------------------------------------------------------------"
    $targetDesc = if ($job.allLocations -or $AllLocations) { "all configured cities" } else { $job.locationKey }
    Write-Output "[$jobIndex/$($jobs.Count)] Running targeted backfill for $targetDesc ($($job.startDate) to $($job.endDate))..."
    Write-Output "------------------------------------------------------------"

    $backfillParams = @{
        StartDate = $job.startDate
        EndDate   = $job.endDate
    }
    if ($job.allLocations -or $AllLocations) {
        $backfillParams["AllLocations"] = $true
    } else {
        $backfillParams["Locations"] = @($job.locationKey)
    }
    if ($BuildProcessor) {
        $backfillParams["BuildProcessor"] = $true
    }

    & $backfillScript @backfillParams
    if ($LASTEXITCODE -ne 0) {
        throw "Targeted backfill failed for $($job.locationKey) on range $($job.startDate) -> $($job.endDate)."
    }

    $jobIndex++
}

Write-Output "`n============================================================"
Write-Output "  Smart Backfill Completed Successfully! All gaps patched.  "
Write-Output "============================================================"
