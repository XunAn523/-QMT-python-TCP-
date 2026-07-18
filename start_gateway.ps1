[CmdletBinding()]
param(
    [string]$EnvFile,
    [string]$PythonExe
)

$ErrorActionPreference = 'Stop'
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not $EnvFile) { $EnvFile = Join-Path $Root '.env' }
elseif (-not [IO.Path]::IsPathRooted($EnvFile)) { $EnvFile = Join-Path $Root $EnvFile }
$EnvFile = [IO.Path]::GetFullPath($EnvFile)
. (Join-Path $Root 'tools\Load-ProjectEnv.ps1')
$Pushed = $false
try {
    $Values = Read-QmtLocalEnv -Path $EnvFile
    if (-not $PythonExe) { $PythonExe = [string]$Values['QMT_LOCAL_PYTHON_EXE'] }
    if (-not $PythonExe) { $PythonExe = 'python' }
    Push-Location $Root
    $Pushed = $true

    $SummaryJson = & $PythonExe -B tools\project_env.py --env-file $EnvFile
    if ($LASTEXITCODE -ne 0) { throw 'Project .env resolution failed.' }
    $Summary = $SummaryJson | ConvertFrom-Json
    $Config = [string]$Summary.gateway_config_path
    $LogDir = [string]$Summary.log_dir
    & $PythonExe -B tools\preflight.py --config $Config --deployment
    if ($LASTEXITCODE -ne 0) { throw 'Gateway preflight failed.' }
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

    $GatewayDirName = ([string][char]0x7F51) + ([char]0x5173)
    $Gateway = Join-Path (Join-Path $Root $GatewayDirName) 'bigqmt_gateway_proxy.py'
    Write-Host 'Local Big QMT TCP Gateway' -ForegroundColor Cyan
    Write-Host "Env:      $EnvFile"
    Write-Host "Listen:   $($Summary.bind_host):$($Summary.tcp_port)"
    Write-Host "Account:  $($Summary.account_name)"
    Write-Host "Runtime:  $($Summary.runtime_dir)"
    Write-Host 'Stop:     press Ctrl+C'
    & $PythonExe -B $Gateway --config $Config --log-dir $LogDir
    if ($LASTEXITCODE -ne 0) { throw "Gateway exited with code $LASTEXITCODE" }
}
finally {
    if ($Pushed) { Pop-Location }
}
