[CmdletBinding()]
param(
    [string]$EnvFile,
    [string]$CoordinatorConfig,
    [switch]$NoWorkers,
    [switch]$Status,
    [switch]$Stop,
    [switch]$Force,
    [int]$StartupTimeoutSeconds = 30
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ProjectRoot = [IO.Path]::GetFullPath((Split-Path -Parent $MyInvocation.MyCommand.Path))
$ProjectBoundary = $ProjectRoot.TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
$RuntimeStateDir = [IO.Path]::GetFullPath((Join-Path $ProjectRoot 'runtime'))
$LauncherStatePath = Join-Path $RuntimeStateDir 'external_windows_state.json'
$CoordinatorStatusPath = Join-Path $RuntimeStateDir 'coordinator_host_status.json'
$ControlTokenPath = Join-Path $RuntimeStateDir 'external_windows_control.token'
$HashAlgorithm = [Security.Cryptography.SHA256]::Create()
try {
    $ProjectRootHash = ([BitConverter]::ToString($HashAlgorithm.ComputeHash([Text.Encoding]::UTF8.GetBytes($ProjectRoot))) -replace '-', '').Substring(0, 24).ToLowerInvariant()
}
finally {
    $HashAlgorithm.Dispose()
}
$LauncherMutexName = 'Local\qmt-local-external-' + $ProjectRootHash

function Resolve-ExternalProjectPath {
    param([Parameter(Mandatory = $true)][string]$Value, [Parameter(Mandatory = $true)][string]$Label)
    if (-not $Value) { throw "$Label is required" }
    $Candidate = if ([IO.Path]::IsPathRooted($Value)) {
        [IO.Path]::GetFullPath($Value)
    } else {
        [IO.Path]::GetFullPath((Join-Path $ProjectRoot $Value))
    }
    if (-not $Candidate.StartsWith($ProjectBoundary, [StringComparison]::OrdinalIgnoreCase)) {
        throw "$Label must remain within the project root"
    }
    return $Candidate
}

function Read-ExternalJson {
    param([Parameter(Mandatory = $true)][string]$Path)
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { return $null }
    try {
        return ([IO.File]::ReadAllText($Path, [Text.UTF8Encoding]::new($false, $true)) | ConvertFrom-Json)
    }
    catch { throw "Invalid JSON state file: $Path" }
}

function Write-ExternalJsonAtomic {
    param([Parameter(Mandatory = $true)][string]$Path, [Parameter(Mandatory = $true)]$Value)
    $Parent = Split-Path -Parent $Path
    New-Item -ItemType Directory -Force -Path $Parent | Out-Null
    $Temporary = Join-Path $Parent ('.' + [IO.Path]::GetFileName($Path) + '.' + [guid]::NewGuid().ToString('N') + '.tmp')
    try {
        [IO.File]::WriteAllText(
            $Temporary,
            ($Value | ConvertTo-Json -Depth 8 -Compress),
            [Text.UTF8Encoding]::new($false)
        )
        Move-Item -LiteralPath $Temporary -Destination $Path -Force
    }
    finally {
        if (Test-Path -LiteralPath $Temporary) { Remove-Item -LiteralPath $Temporary -Force }
    }
}

function Test-ExternalProcessAlive {
    param($ProcessId)
    if ($null -eq $ProcessId) { return $false }
    try {
        $Process = Get-Process -Id ([int]$ProcessId) -ErrorAction Stop
        return -not $Process.HasExited
    }
    catch { return $false }
}

function Wait-ExternalProcessExit {
    param([int]$ProcessId, [int]$TimeoutSeconds = 10)
    $Deadline = [DateTime]::UtcNow.AddSeconds([Math]::Max(0, $TimeoutSeconds))
    while (Test-ExternalProcessAlive $ProcessId) {
        if ([DateTime]::UtcNow -ge $Deadline) { return $false }
        Start-Sleep -Milliseconds 100
    }
    return $true
}

function Stop-ExternalProcess {
    param([int]$ProcessId, [switch]$ForceStop)
    if (-not (Test-ExternalProcessAlive $ProcessId)) { return $true }
    try {
        if ($ForceStop) { Stop-Process -Id $ProcessId -Force -ErrorAction Stop }
        else { Stop-Process -Id $ProcessId -ErrorAction Stop }
    }
    catch { return $false }
    return (Wait-ExternalProcessExit -ProcessId $ProcessId -TimeoutSeconds 10)
}

function New-ExternalControlToken {
    New-Item -ItemType Directory -Force -Path $RuntimeStateDir | Out-Null
    $Bytes = New-Object byte[] 32
    $Generator = [Security.Cryptography.RandomNumberGenerator]::Create()
    try { $Generator.GetBytes($Bytes) }
    finally { $Generator.Dispose() }
    [IO.File]::WriteAllBytes($ControlTokenPath, $Bytes)
    $Identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
    & icacls.exe $ControlTokenPath '/inheritance:r' ('/grant:r') ($Identity + ':(R,W)') 'SYSTEM:(F)' | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not restrict the Coordinator control-token ACL.' }
}

function Enter-ExternalLauncherMutex {
    $Mutex = New-Object Threading.Mutex($false, $LauncherMutexName)
    try {
        if (-not $Mutex.WaitOne(0)) {
            $Mutex.Dispose()
            throw 'A managed external Windows launcher instance is already active. Use -Status or -Stop.'
        }
    }
    catch [Threading.AbandonedMutexException] {
        # The previous launcher died without releasing the mutex.  Ownership is
        # now ours; persistent PID checks below still prevent an unsafe double start.
    }
    return $Mutex
}

function Exit-ExternalLauncherMutex {
    param($Mutex)
    if ($null -eq $Mutex) { return }
    try { $Mutex.ReleaseMutex() } catch [ApplicationException] { }
    $Mutex.Dispose()
}

function Wait-ExternalTcpPort {
    param([Parameter(Mandatory = $true)][string]$Host, [Parameter(Mandatory = $true)][int]$Port, [Parameter(Mandatory = $true)][int]$TimeoutSeconds)
    $Deadline = [DateTime]::UtcNow.AddSeconds([Math]::Max(1, $TimeoutSeconds))
    while ([DateTime]::UtcNow -lt $Deadline) {
        $Client = New-Object Net.Sockets.TcpClient
        try {
            $Async = $Client.BeginConnect($Host, $Port, $null, $null)
            if ($Async.AsyncWaitHandle.WaitOne(300) -and $Client.Connected) {
                $Client.EndConnect($Async)
                return $true
            }
        }
        catch { }
        finally { $Client.Close() }
        Start-Sleep -Milliseconds 150
    }
    return $false
}

function ConvertTo-ExternalArgumentLine {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)
    # Config validation rejects quotes and NUL. Quoting every field makes spaces
    # in project paths and worker arguments unambiguous without shell parsing.
    # Windows command-line parsing treats a backslash before the closing quote
    # specially, so double only a trailing run of backslashes.
    return (($Arguments | ForEach-Object {
        $Escaped = $_ -replace '(\\+)$', '$1$1'
        '"' + $Escaped + '"'
    }) -join ' ')
}

function Start-ExternalChild {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$WorkingDirectory,
        [Parameter(Mandatory = $true)][string]$StdoutPath,
        [Parameter(Mandatory = $true)][string]$StderrPath
    )
    return Start-Process -FilePath $FilePath `
        -ArgumentList (ConvertTo-ExternalArgumentLine $Arguments) `
        -WorkingDirectory $WorkingDirectory `
        -WindowStyle Hidden `
        -RedirectStandardOutput $StdoutPath `
        -RedirectStandardError $StderrPath `
        -PassThru
}

function Invoke-ExternalCoordinatorControl {
    param([Parameter(Mandatory = $true)]$State, [Parameter(Mandatory = $true)][string]$Command, [Parameter(Mandatory = $true)][string]$PythonExe, [Parameter(Mandatory = $true)][string]$HostScript)
    if (-not $State.control_pipe -or -not (Test-Path -LiteralPath $ControlTokenPath -PathType Leaf)) {
        throw 'Coordinator control channel is unavailable'
    }
    $Raw = & $PythonExe -B $HostScript --config $CoordinatorConfig --control-pipe ([string]$State.control_pipe) --control-token-file $ControlTokenPath --send-control $Command
    if ($LASTEXITCODE -ne 0) { throw 'Coordinator control command failed' }
    return ($Raw | Select-Object -Last 1 | ConvertFrom-Json)
}

function Remove-ExternalRuntimeState {
    foreach ($Path in @($LauncherStatePath, $CoordinatorStatusPath, $ControlTokenPath)) {
        if (Test-Path -LiteralPath $Path) { Remove-Item -LiteralPath $Path -Force }
    }
}

function Stop-ExternalWindows {
    param([switch]$ForceStop)
    $State = Read-ExternalJson $LauncherStatePath
    if ($null -eq $State) {
        Write-Host 'No managed external Windows process state was found.' -ForegroundColor Yellow
        return
    }
    $VenvPython = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
    $ApiRoot = Join-Path $ProjectRoot (([string][char]0x5916) + ([char]0x7F6E) + ([char]0x7B56) + ([char]0x7565) + 'API')
    $HostScript = Join-Path (Join-Path $ApiRoot 'qmt_local_api') 'coordinator_host.py'
    foreach ($Entry in @($State.worker_pids)) {
        if ($Entry -and (Test-ExternalProcessAlive $Entry.pid)) {
            [void](Stop-ExternalProcess -ProcessId ([int]$Entry.pid) -ForceStop:$ForceStop)
        }
    }
    # Workers are intentionally unable to connect to Gateway directly.  Stop
    # them before closing the sole Coordinator endpoint so they cannot emit a
    # new signal during the rest of the shutdown sequence.
    Start-Sleep -Milliseconds 300
    $CoordinatorStopped = $false
    if (Test-ExternalProcessAlive $State.coordinator_pid) {
        try {
            $Response = Invoke-ExternalCoordinatorControl -State $State -Command 'SHUTDOWN' -PythonExe $VenvPython -HostScript $HostScript
            $CoordinatorStopped = [bool]$Response.stopping -and (Wait-ExternalProcessExit -ProcessId ([int]$State.coordinator_pid) -TimeoutSeconds 12)
        }
        catch { $CoordinatorStopped = $false }
    }
    if (-not $CoordinatorStopped -and (Test-ExternalProcessAlive $State.coordinator_pid)) {
        if (-not $ForceStop) { throw 'Coordinator did not stop through its authenticated control pipe. Re-run with -Force after investigation.' }
        [void](Stop-ExternalProcess -ProcessId ([int]$State.coordinator_pid) -ForceStop)
    }
    if (Test-ExternalProcessAlive $State.gateway_pid) {
        [void](Stop-ExternalProcess -ProcessId ([int]$State.gateway_pid) -ForceStop:$ForceStop)
    }
    $LiveWorker = $false
    foreach ($Entry in @($State.worker_pids)) {
        if ($Entry -and (Test-ExternalProcessAlive $Entry.pid)) { $LiveWorker = $true; break }
    }
    if ((Test-ExternalProcessAlive $State.gateway_pid) -or (Test-ExternalProcessAlive $State.coordinator_pid) -or $LiveWorker) {
        throw 'One or more managed external processes are still active; state files were retained for investigation.'
    }
    Remove-ExternalRuntimeState
    Write-Host 'Managed external Windows processes stopped.' -ForegroundColor Green
}

if (($Status -and $Stop) -or (($Status -or $Stop) -and $NoWorkers)) {
    throw 'Select only one of -Status, -Stop, or normal start options.'
}
if ($StartupTimeoutSeconds -lt 5 -or $StartupTimeoutSeconds -gt 300) {
    throw 'StartupTimeoutSeconds must be in 5..300'
}
if (-not $EnvFile) { $EnvFile = '.env' }
if (-not $CoordinatorConfig) { $CoordinatorConfig = 'coordinator_config.json' }
$EnvFile = Resolve-ExternalProjectPath -Value $EnvFile -Label 'EnvFile'
$CoordinatorConfig = Resolve-ExternalProjectPath -Value $CoordinatorConfig -Label 'CoordinatorConfig'

if ($Status) {
    $State = Read-ExternalJson $LauncherStatePath
    if ($null -eq $State) { Write-Output '{"running":false}'; exit 0 }
    $State | Add-Member -NotePropertyName gateway_pid_alive -NotePropertyValue (Test-ExternalProcessAlive $State.gateway_pid) -Force
    $State | Add-Member -NotePropertyName coordinator_pid_alive -NotePropertyValue (Test-ExternalProcessAlive $State.coordinator_pid) -Force
    $State | Add-Member -NotePropertyName worker_pid_alive -NotePropertyValue @(
        foreach ($Entry in @($State.worker_pids)) {
            [pscustomobject]@{
                strategy_id = [string]$Entry.strategy_id
                alive = Test-ExternalProcessAlive $Entry.pid
            }
        }
    ) -Force
    if ($State.coordinator_pid_alive) {
        $VenvPython = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
        $ApiRoot = Join-Path $ProjectRoot (([string][char]0x5916) + ([char]0x7F6E) + ([char]0x7B56) + ([char]0x7565) + 'API')
        $HostScript = Join-Path (Join-Path $ApiRoot 'qmt_local_api') 'coordinator_host.py'
        if (Test-Path -LiteralPath $VenvPython -PathType Leaf) {
            try {
                $State | Add-Member -NotePropertyName coordinator_health -NotePropertyValue (Invoke-ExternalCoordinatorControl -State $State -Command 'STATUS' -PythonExe $VenvPython -HostScript $HostScript) -Force
            }
            catch {
                $State | Add-Member -NotePropertyName coordinator_health_error -NotePropertyValue 'unavailable' -Force
            }
        }
    }
    $State | ConvertTo-Json -Depth 8
    exit 0
}
if ($Stop) {
    Stop-ExternalWindows -ForceStop:$Force
    exit 0
}

if (-not (Test-Path -LiteralPath $EnvFile -PathType Leaf)) { throw "Environment file not found: $EnvFile" }
if (-not (Test-Path -LiteralPath $CoordinatorConfig -PathType Leaf)) { throw "Coordinator config not found: $CoordinatorConfig" }
if (-not (Test-Path -LiteralPath (Join-Path $ProjectRoot '.venv\Scripts\python.exe') -PathType Leaf)) {
    & (Join-Path $ProjectRoot 'setup_venv.ps1')
    if ($LASTEXITCODE -ne 0) { throw 'Project virtual environment setup failed.' }
}
. (Join-Path $ProjectRoot 'tools\Resolve-ProjectPython.ps1')
$PythonExe = Resolve-QmtProjectPython -ProjectRoot $ProjectRoot -EnvFile $EnvFile
$ApiRoot = Join-Path $ProjectRoot (([string][char]0x5916) + ([char]0x7F6E) + ([char]0x7B56) + ([char]0x7565) + 'API')
$HostScript = Join-Path (Join-Path $ApiRoot 'qmt_local_api') 'coordinator_host.py'
$ConfigRaw = & $PythonExe -B $HostScript --config $CoordinatorConfig --check-config
if ($LASTEXITCODE -ne 0) { throw 'Coordinator configuration validation failed.' }
$LaunchPlan = $ConfigRaw | Select-Object -Last 1 | ConvertFrom-Json

$Existing = Read-ExternalJson $LauncherStatePath
if ($null -ne $Existing -and ((Test-ExternalProcessAlive $Existing.gateway_pid) -or (Test-ExternalProcessAlive $Existing.coordinator_pid))) {
    throw 'A managed external Windows instance is already active. Use -Status or -Stop.'
}
Remove-ExternalRuntimeState

$SummaryRaw = & $PythonExe -B tools\project_env.py --env-file $EnvFile
if ($LASTEXITCODE -ne 0) { throw 'Project .env resolution failed.' }
$Summary = $SummaryRaw | Select-Object -Last 1 | ConvertFrom-Json
& $PythonExe -B tools\preflight.py --config ([string]$Summary.gateway_config_path) --deployment
if ($LASTEXITCODE -ne 0) { throw 'Gateway and Helper deployment preflight failed.' }

$LogDir = [string]$Summary.log_dir
New-Item -ItemType Directory -Force -Path $LogDir, $RuntimeStateDir | Out-Null
$GatewayDir = Join-Path $ProjectRoot (([string][char]0x7F51) + ([char]0x5173))
$GatewayScript = Join-Path $GatewayDir 'bigqmt_gateway_proxy.py'
$Gateway = $null
$Coordinator = $null
$Workers = @()
$PipeName = 'qmt-local-coordinator-' + [guid]::NewGuid().ToString('N')
$LauncherMutex = Enter-ExternalLauncherMutex

try {
    New-ExternalControlToken
    $Gateway = Start-ExternalChild -FilePath $PythonExe -Arguments @(
        '-B', $GatewayScript, '--config', ([string]$Summary.gateway_config_path), '--log-dir', $LogDir
    ) -WorkingDirectory $ProjectRoot -StdoutPath (Join-Path $LogDir 'gateway-external.stdout.log') -StderrPath (Join-Path $LogDir 'gateway-external.stderr.log')
    if (-not (Wait-ExternalTcpPort -Host ([string]$Summary.bind_host) -Port ([int]$Summary.tcp_port) -TimeoutSeconds $StartupTimeoutSeconds)) {
        throw 'Gateway TCP port did not become available.'
    }

    $Coordinator = Start-ExternalChild -FilePath $PythonExe -Arguments @(
        '-B', $HostScript, '--env-file', $EnvFile, '--config', $CoordinatorConfig,
        '--status-file', $CoordinatorStatusPath, '--control-pipe', $PipeName,
        '--control-token-file', $ControlTokenPath
    ) -WorkingDirectory $ProjectRoot -StdoutPath (Join-Path $LogDir 'coordinator.stdout.log') -StderrPath (Join-Path $LogDir 'coordinator.stderr.log')

    $Deadline = [DateTime]::UtcNow.AddSeconds($StartupTimeoutSeconds)
    $CoordinatorReady = $false
    while ([DateTime]::UtcNow -lt $Deadline) {
        if ($Coordinator.HasExited) { throw 'Coordinator Host exited before reaching ready state.' }
        $HostStatus = Read-ExternalJson $CoordinatorStatusPath
        if ($HostStatus -and [bool]$HostStatus.coordinator_ready -and [string]$HostStatus.control_pipe -eq $PipeName) {
            $CoordinatorReady = $true
            break
        }
        Start-Sleep -Milliseconds 200
    }
    if (-not $CoordinatorReady) { throw 'Coordinator Host did not reach ready state.' }

    if (-not $NoWorkers) {
        foreach ($Worker in @($LaunchPlan.workers)) {
            if (-not [bool]$Worker.enabled) { continue }
            if (-not (Test-Path -LiteralPath ([string]$Worker.program) -PathType Leaf)) { throw "Configured worker program was not found: $($Worker.strategy_id)" }
            if (-not (Test-Path -LiteralPath ([string]$Worker.working_directory) -PathType Container)) { throw "Configured worker directory was not found: $($Worker.strategy_id)" }
            $Process = Start-ExternalChild -FilePath $PythonExe -Arguments (@('-B', [string]$Worker.program) + @($Worker.arguments)) `
                -WorkingDirectory ([string]$Worker.working_directory) `
                -StdoutPath (Join-Path $LogDir ($Worker.strategy_id + '.stdout.log')) `
                -StderrPath (Join-Path $LogDir ($Worker.strategy_id + '.stderr.log'))
            $Workers += [pscustomobject]@{ strategy_id = [string]$Worker.strategy_id; pid = $Process.Id }
        }
    }

    Write-ExternalJsonAtomic -Path $LauncherStatePath -Value ([ordered]@{
        version = 1
        launcher_pid = $PID
        gateway_pid = $Gateway.Id
        coordinator_pid = $Coordinator.Id
        worker_pids = $Workers
        gateway_endpoint = ([string]$Summary.bind_host + ':' + [string]$Summary.tcp_port)
        coordinator_endpoint = ([string]$LaunchPlan.server.host + ':' + [string]$LaunchPlan.server.port)
        control_pipe = $PipeName
        started_at = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
        gateway_ready = $true
        coordinator_ready = $true
    })
    Write-Host 'External Windows environment is ready.' -ForegroundColor Green
    Write-Host "Gateway:     $($Summary.bind_host):$($Summary.tcp_port)"
    Write-Host "Coordinator: $($LaunchPlan.server.host):$($LaunchPlan.server.port)"
    Write-Host 'Stop:        Ctrl+C, or .\start_external_windows.ps1 -Stop'

    while ($true) {
        if ($Gateway.HasExited) { throw 'Gateway process exited unexpectedly.' }
        if ($Coordinator.HasExited) { throw 'Coordinator Host process exited unexpectedly.' }
        foreach ($Worker in $Workers) {
            if (-not (Test-ExternalProcessAlive $Worker.pid)) { throw "Worker exited unexpectedly: $($Worker.strategy_id)" }
        }
        Start-Sleep -Seconds 1
    }
}
finally {
    if ($null -ne $Coordinator -or $null -ne $Gateway) {
        # A failed launch may not have written its persistent state yet.  Clean
        # the children held by this invocation directly, then remove state only
        # when no managed child remains alive.
        foreach ($Worker in $Workers) {
            [void](Stop-ExternalProcess -ProcessId ([int]$Worker.pid) -ForceStop:$Force)
        }
        if ($Workers.Count) { Start-Sleep -Milliseconds 300 }
        if ($null -ne $Coordinator -and (Test-ExternalProcessAlive $Coordinator.Id)) {
            try {
                $TransientState = [pscustomobject]@{ control_pipe = $PipeName }
                [void](Invoke-ExternalCoordinatorControl -State $TransientState -Command 'SHUTDOWN' -PythonExe $PythonExe -HostScript $HostScript)
                [void](Wait-ExternalProcessExit -ProcessId $Coordinator.Id -TimeoutSeconds 12)
            }
            catch { Write-Warning 'Coordinator did not acknowledge graceful shutdown during cleanup.' }
        }
        if ($null -ne $Coordinator -and (Test-ExternalProcessAlive $Coordinator.Id)) {
            if ($Force) { [void](Stop-ExternalProcess -ProcessId $Coordinator.Id -ForceStop) }
            else { Write-Warning 'Coordinator process remains active; use -Stop -Force after investigation.' }
        }
        if (($null -eq $Coordinator -or -not (Test-ExternalProcessAlive $Coordinator.Id)) -and
            $null -ne $Gateway -and (Test-ExternalProcessAlive $Gateway.Id)) {
            [void](Stop-ExternalProcess -ProcessId $Gateway.Id -ForceStop:$Force)
        }
        $LiveWorker = $false
        foreach ($Worker in $Workers) {
            if (Test-ExternalProcessAlive $Worker.pid) { $LiveWorker = $true; break }
        }
        if (($null -eq $Coordinator -or -not (Test-ExternalProcessAlive $Coordinator.Id)) -and
            ($null -eq $Gateway -or -not (Test-ExternalProcessAlive $Gateway.Id)) -and -not $LiveWorker) {
            Remove-ExternalRuntimeState
        }
    }
    Exit-ExternalLauncherMutex $LauncherMutex
}
