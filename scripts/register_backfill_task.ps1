param(
    [string]$TaskName = "Climate Monitoring - Incremental Backfill",
    [ValidateRange(1, 31)]
    [int]$LookbackDays = 7,
    [switch]$Unregister,
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$backfillScript = Join-Path $PSScriptRoot "run_climate_backfill.ps1"
$taskArguments = "-NoProfile -ExecutionPolicy Bypass -File `"$backfillScript`" -LookbackDays $LookbackDays"

if ($WhatIf) {
    Write-Output $(if ($Unregister) {
        "Would unregister scheduled backfill task '$TaskName'."
    } else {
        "Would register '$TaskName' daily at 03:00 with a $LookbackDays-day UTC lookback."
    })
    exit 0
}

$scheduler = New-Object -ComObject "Schedule.Service"
$scheduler.Connect()
$rootFolder = $scheduler.GetFolder("\")

if ($Unregister) {
    try {
        $rootFolder.DeleteTask($TaskName, 0)
        Write-Output "Unregistered scheduled task '$TaskName'."
    } catch {
        if ($_.Exception.Message -match "cannot find|does not exist|not found") {
            Write-Output "Scheduled task '$TaskName' is not registered."
        } else {
            throw
        }
    }
    exit 0
}

$definition = $scheduler.NewTask(0)
$definition.RegistrationInfo.Description = "Incrementally backfill weather and Open-Meteo air quality data, then run the historical Spark batch."
$definition.Settings.Enabled = $true
$definition.Settings.StartWhenAvailable = $true
$definition.Settings.MultipleInstances = 2
$definition.Settings.ExecutionTimeLimit = "PT6H"
$definition.Settings.DisallowStartIfOnBatteries = $false
$definition.Settings.StopIfGoingOnBatteries = $false
$definition.Principal.UserId = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$definition.Principal.LogonType = 3
$definition.Principal.RunLevel = 0

$trigger = $definition.Triggers.Create(2)
$trigger.StartBoundary = (Get-Date).Date.AddDays(1).AddHours(3).ToString("yyyy-MM-dd'T'HH:mm:ss")
$trigger.DaysInterval = 1

$action = $definition.Actions.Create(0)
$action.Path = Join-Path $PSHOME "powershell.exe"
$action.Arguments = $taskArguments
$action.WorkingDirectory = $repositoryRoot

$null = $rootFolder.RegisterTaskDefinition(
    $TaskName,
    $definition,
    6,
    $definition.Principal.UserId,
    $null,
    3,
    $null
)

Write-Output "Registered '$TaskName' daily at 03:00 with a $LookbackDays-day UTC lookback."
