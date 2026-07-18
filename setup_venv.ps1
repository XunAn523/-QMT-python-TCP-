[CmdletBinding()]
param(
    [string]$BootstrapPython,
    [switch]$Recreate
)

$ErrorActionPreference = 'Stop'
$Root = [IO.Path]::GetFullPath((Split-Path -Parent $MyInvocation.MyCommand.Path))
$VenvDir = [IO.Path]::GetFullPath((Join-Path $Root '.venv'))
$VenvPython = Join-Path $VenvDir 'Scripts\python.exe'
$RootBoundary = $Root.TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
if (
    -not $VenvDir.StartsWith($RootBoundary, [StringComparison]::OrdinalIgnoreCase) `
    -or [IO.Path]::GetFileName($VenvDir) -ne '.venv'
) {
    throw "Unsafe virtual-environment path: $VenvDir"
}

if ($Recreate -and (Test-Path -LiteralPath $VenvDir)) {
    Remove-Item -LiteralPath $VenvDir -Recurse -Force
}

$IdentityCode = @'
import json, platform, struct, sys
print(json.dumps({
    'platform': platform.system(),
    'implementation': sys.implementation.name,
    'major': sys.version_info[0],
    'minor': sys.version_info[1],
    'bits': struct.calcsize('P') * 8,
    'in_venv': sys.prefix != sys.base_prefix,
    'executable': sys.executable,
}, sort_keys=True))
'@

$CreatedThisRun = $false
try {
    if (-not (Test-Path -LiteralPath $VenvPython -PathType Leaf)) {
        if (Test-Path -LiteralPath $VenvDir) {
            throw "The existing .venv is incomplete. Run .\setup_venv.ps1 -Recreate: $VenvDir"
        }

        $BootstrapArgs = @()
        if ($BootstrapPython) {
            if ([IO.Path]::IsPathRooted($BootstrapPython) -or $BootstrapPython.Contains('\') -or $BootstrapPython.Contains('/')) {
                $Bootstrap = [IO.Path]::GetFullPath($BootstrapPython)
                if (-not (Test-Path -LiteralPath $Bootstrap -PathType Leaf)) {
                    throw "Bootstrap Python was not found: $Bootstrap"
                }
            }
            else {
                $Bootstrap = (Get-Command -Name $BootstrapPython -CommandType Application -ErrorAction Stop).Source
            }
        }
        else {
            $Launcher = Get-Command -Name 'py' -CommandType Application -ErrorAction SilentlyContinue
            if ($null -ne $Launcher) {
                $Bootstrap = $Launcher.Source
                $BootstrapArgs = @('-3.12')
            }
            else {
                $Bootstrap = (Get-Command -Name 'python' -CommandType Application -ErrorAction Stop).Source
            }
        }

        $BootstrapIdentityRaw = & $Bootstrap @BootstrapArgs -I -c $IdentityCode
        if ($LASTEXITCODE -ne 0) {
            throw 'Windows CPython 3.12 x64 was not found. Install it, then rerun setup_venv.ps1.'
        }
        $BootstrapIdentity = $BootstrapIdentityRaw | Select-Object -Last 1 | ConvertFrom-Json
        if (
            $BootstrapIdentity.platform -ne 'Windows' `
            -or $BootstrapIdentity.implementation -ne 'cpython' `
            -or [int]$BootstrapIdentity.major -ne 3 `
            -or [int]$BootstrapIdentity.minor -ne 12 `
            -or [int]$BootstrapIdentity.bits -ne 64
        ) {
            throw 'Virtual-environment bootstrap requires Windows CPython 3.12 x64.'
        }

        $CreatedThisRun = $true
        & $Bootstrap @BootstrapArgs -I -m venv $VenvDir
        if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $VenvPython -PathType Leaf)) {
            throw 'Failed to create the project .venv.'
        }
    }

    $VenvIdentityRaw = & $VenvPython -I -c $IdentityCode
    if ($LASTEXITCODE -ne 0) { throw 'The project .venv Python cannot start.' }
    $VenvIdentity = $VenvIdentityRaw | Select-Object -Last 1 | ConvertFrom-Json
    $ActualVenvPython = [IO.Path]::GetFullPath([string]$VenvIdentity.executable)
    if (
        $VenvIdentity.platform -ne 'Windows' `
        -or $VenvIdentity.implementation -ne 'cpython' `
        -or [int]$VenvIdentity.major -ne 3 `
        -or [int]$VenvIdentity.minor -ne 12 `
        -or [int]$VenvIdentity.bits -ne 64 `
        -or -not [bool]$VenvIdentity.in_venv `
        -or -not $ActualVenvPython.Equals(
            [IO.Path]::GetFullPath($VenvPython),
            [StringComparison]::OrdinalIgnoreCase
        )
    ) {
        throw 'The existing .venv is not this project Windows CPython 3.12 x64 environment. Use -Recreate.'
    }

    $SitePackages = Join-Path $VenvDir 'Lib\site-packages'
    if (-not (Test-Path -LiteralPath $SitePackages -PathType Container)) {
        throw "The project .venv site-packages directory is missing: $SitePackages"
    }
    $PthPath = Join-Path $SitePackages 'qmt_local_strategy_api.pth'
    $PthLine = 'import os,sys; sys.path.insert(0, os.path.normpath(os.path.join(sys.prefix, os.pardir, "\u5916\u7f6e\u7b56\u7565API")))'
    [IO.File]::WriteAllText(
        $PthPath,
        $PthLine + [Environment]::NewLine,
        [Text.ASCIIEncoding]::new()
    )

    $ApiDirName = ([string][char]0x5916) + ([char]0x7F6E) + ([char]0x7B56) + ([char]0x7565) + 'API'
    $ExpectedApi = [IO.Path]::GetFullPath(
        (Join-Path (Join-Path (Join-Path $Root $ApiDirName) 'qmt_local_api') '__init__.py')
    )
    $VerifyCode = @'
import os, sys
import qmt_local_api
actual = os.path.normcase(os.path.realpath(qmt_local_api.__file__))
expected = os.path.normcase(os.path.realpath(sys.argv[1]))
if actual != expected:
    raise SystemExit('qmt_local_api source mismatch: %s != %s' % (actual, expected))
'@
    & $VenvPython -B -c $VerifyCode $ExpectedApi
    if ($LASTEXITCODE -ne 0) { throw 'The local strategy API .pth verification failed.' }

    Write-Host "Project virtual environment ready: $VenvPython" -ForegroundColor Green
    Write-Host 'Local qmt_local_api is available without downloading dependencies.' -ForegroundColor Green
}
catch {
    if ($CreatedThisRun -and (Test-Path -LiteralPath $VenvDir)) {
        Remove-Item -LiteralPath $VenvDir -Recurse -Force
    }
    throw
}
