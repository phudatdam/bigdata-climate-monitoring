param(
    [string]$TaskName = "Climate Monitoring - Collect",
    [string[]]$Locations = @(),
    [ValidateRange(1, 1440)]
    [int]$IntervalMinutes = 15,
    [switch]$Unregister,
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$collectorScript = Join-Path $PSScriptRoot "collect_once.ps1"
$taskArguments = "-NoProfile -ExecutionPolicy Bypass -File `"$collectorScript`""
if ($Locations.Count -gt 0) {
    $taskArguments += " -Locations"
    foreach ($location in $Locations) {
        if ($location -notmatch '^[\p{L}\p{M}\d -]+$') {
            throw "Unsupported location value for a scheduled task: $location"
        }
        $taskArguments += " `"$location`""
    }
}

if ($WhatIf) {
    if ($Unregister) {
        Write-Output "Would unregister scheduled task '$TaskName'."
    } else {
        Write-Output "Would register '$TaskName' to run $collectorScript every $IntervalMinutes minutes for locations: $($Locations -join ', '). Empty locations means all configured provider locations."
        Write-Output "The task runs as $([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) when that account is logged on."
    }
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
$definition.RegistrationInfo.Description = "Poll configured climate providers, validate and publish to Kafka. Collector API credentials remain in the ingestion environment."
$definition.Settings.Enabled = $true
$definition.Settings.StartWhenAvailable = $true
$definition.Settings.MultipleInstances = 2
$definition.Settings.ExecutionTimeLimit = "PT2H"
$definition.Settings.DisallowStartIfOnBatteries = $false
$definition.Settings.StopIfGoingOnBatteries = $false
$definition.Principal.UserId = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$definition.Principal.LogonType = 3
$definition.Principal.RunLevel = 0

$trigger = $definition.Triggers.Create(2)
$trigger.StartBoundary = (Get-Date).AddMinutes(1).ToString("yyyy-MM-dd'T'HH:mm:ss")
$trigger.DaysInterval = 1
$trigger.Repetition.Interval = "PT${IntervalMinutes}M"
$trigger.Repetition.Duration = "P1D"
$trigger.Repetition.StopAtDurationEnd = $false

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

Write-Output "Registered '$TaskName' at a $IntervalMinutes-minute cadence for the logged-in user."
Write-Output "Locations: $(if ($Locations.Count) { $Locations -join ', ' } else { 'all configured provider locations' })"
Write-Output "The task ignores overlapping runs and writes run summaries under logs\collection\."
