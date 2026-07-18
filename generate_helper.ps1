[CmdletBinding()]
param(
    [string]$EnvFile,
    [string]$PythonExe,
    [switch]$AllowExample,
    [switch]$Deploy,
    [switch]$ConfirmStoppedStrategy
)

$ErrorActionPreference = 'Stop'
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not $EnvFile) { $EnvFile = Join-Path $Root '.env' }
elseif (-not [IO.Path]::IsPathRooted($EnvFile)) { $EnvFile = Join-Path $Root $EnvFile }
$EnvFile = [IO.Path]::GetFullPath($EnvFile)
. (Join-Path $Root 'tools\Resolve-ProjectPython.ps1')
$OfflineBuild = $null
try {
    if ($Deploy -and $AllowExample) { throw 'Example accounts can never be deployed.' }
    if ($Deploy -and -not $ConfirmStoppedStrategy) {
        throw 'Deployment requires -ConfirmStoppedStrategy after the QMT strategy is stopped.'
    }
    $PythonExe = Resolve-QmtProjectPython `
        -ProjectRoot $Root `
        -EnvFile $EnvFile `
        -PythonExe $PythonExe

    $SummaryArgs = @('-B', (Join-Path $Root 'tools\project_env.py'), '--env-file', $EnvFile, '--describe')
    if ($AllowExample) { $SummaryArgs += '--allow-example' }
    $SummaryJson = & $PythonExe @SummaryArgs
    if ($LASTEXITCODE -ne 0) { throw 'Project .env resolution failed.' }
    $Summary = $SummaryJson | ConvertFrom-Json
    if ($AllowExample) {
        $OfflineBuild = Join-Path ([IO.Path]::GetTempPath()) ('qmt-local-helper-example-' + [guid]::NewGuid().ToString('N'))
        $OutputDir = [IO.Path]::GetFullPath($OfflineBuild)
    }
    else {
        $OutputDir = [IO.Path]::GetFullPath([string]$Summary.helper_output_dir)
    }
    $QmtDirName = ([string][char]0x5927) + 'QMT' + ([char]0x5185) + ([char]0x7F6E) + 'python'
    $Generator = Join-Path (Join-Path $Root $QmtDirName) 'tools\generate_helpers.py'
    $Arguments = @('-B', $Generator, '--env-file', $EnvFile, '--output', $OutputDir)
    if ($AllowExample) { $Arguments += @('--allow-example', '--ignore-process-env') }
    & $PythonExe @Arguments
    if ($LASTEXITCODE -ne 0) { throw 'QMT helper generation failed.' }
    & $PythonExe @Arguments --check
    if ($LASTEXITCODE -ne 0) { throw 'QMT helper deterministic check failed.' }
    Write-Host "Helper generated and verified: $OutputDir" -ForegroundColor Green

    if ($Deploy) {
        $InstallRoot = [IO.Path]::GetFullPath([string]$Summary.helper_install_root)
        $AccountName = [string]$Summary.account_name
        New-Item -ItemType Directory -Force -Path $InstallRoot | Out-Null
        $Source = [IO.Path]::GetFullPath((Join-Path $OutputDir $AccountName))
        $Target = [IO.Path]::GetFullPath((Join-Path $InstallRoot $AccountName))
        $Boundary = $InstallRoot.TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
        if (-not $Target.StartsWith($Boundary, [StringComparison]::OrdinalIgnoreCase)) {
            throw "Helper deployment escaped install root: $Target"
        }
        foreach ($Required in @('bigqmt_file_queue_helper.py', 'bigqmt_loader.py')) {
            if (-not (Test-Path -LiteralPath (Join-Path $Source $Required) -PathType Leaf)) {
                throw "Generated helper is incomplete: $Source"
            }
        }
        $Token = [guid]::NewGuid().ToString('N')
        $Staging = Join-Path $InstallRoot ('.' + $AccountName + '.staging.' + $Token)
        $Backup = Join-Path $InstallRoot ('.' + $AccountName + '.backup.' + $Token)
        Copy-Item -LiteralPath $Source -Destination $Staging -Recurse
        $MovedOld = $false
        try {
            if (Test-Path -LiteralPath $Target) {
                Move-Item -LiteralPath $Target -Destination $Backup
                $MovedOld = $true
            }
            Move-Item -LiteralPath $Staging -Destination $Target
            if ($MovedOld) {
                try { Remove-Item -LiteralPath $Backup -Recurse -Force }
                catch {
                    Write-Warning "Helper deployment committed, but the old backup could not be removed: $Backup"
                }
            }
        }
        catch {
            if (Test-Path -LiteralPath $Staging) { Remove-Item -LiteralPath $Staging -Recurse -Force }
            if ($MovedOld -and -not (Test-Path -LiteralPath $Target) -and (Test-Path -LiteralPath $Backup)) {
                Move-Item -LiteralPath $Backup -Destination $Target
            }
            throw
        }
        Write-Host "Helper deployed: $Target" -ForegroundColor Cyan
        Write-Host 'Reload the stopped QMT strategy with bigqmt_loader.py.' -ForegroundColor Yellow
    }
}
finally {
    if ($null -ne $OfflineBuild -and (Test-Path -LiteralPath $OfflineBuild)) {
        Remove-Item -LiteralPath $OfflineBuild -Recurse -Force
    }
}
