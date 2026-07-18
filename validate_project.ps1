[CmdletBinding()]
param([string]$PythonExe)

$ErrorActionPreference = 'Stop'
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $Root 'tools\Resolve-ProjectPython.ps1')
$PythonExe = Resolve-QmtProjectPython -ProjectRoot $Root -PythonExe $PythonExe
$Generated = Join-Path ([IO.Path]::GetTempPath()) ('qmt-local-project-' + [guid]::NewGuid().ToString('N'))
$PreviousDontWriteBytecode = [Environment]::GetEnvironmentVariable('PYTHONDONTWRITEBYTECODE', 'Process')
$Pushed = $false
try {
    [Environment]::SetEnvironmentVariable('PYTHONDONTWRITEBYTECODE', '1', 'Process')
    Push-Location $Root
    $Pushed = $true

    $SyntaxCheck = @'
import ast
from pathlib import Path

excluded = {'.git', '.venv', '__pycache__'}
paths = sorted(
    path for path in Path('.').rglob('*.py')
    if not excluded.intersection(path.parts)
)
for path in paths:
    ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
print('syntax_check=ok files=%d' % len(paths))
'@
    & $PythonExe -B -c $SyntaxCheck
    if ($LASTEXITCODE -ne 0) { throw 'Python syntax validation failed.' }

    $SummaryJson = & $PythonExe -B tools\project_env.py --env-file .env.example --output-dir $Generated --allow-example --ignore-process-env
    if ($LASTEXITCODE -ne 0) { throw 'Root .env.example materialization failed.' }
    $Summary = $SummaryJson | ConvertFrom-Json
    & $PythonExe -B tools\preflight.py --config ([string]$Summary.gateway_config_path) --allow-example
    if ($LASTEXITCODE -ne 0) { throw 'Offline preflight failed.' }

    & $PythonExe -B -m unittest discover -s tests -v
    if ($LASTEXITCODE -ne 0) { throw 'Root Gateway/env tests failed.' }
    & powershell -NoProfile -ExecutionPolicy Bypass -File tests\test_env_loader.ps1
    if ($LASTEXITCODE -ne 0) { throw 'PowerShell .env parser test failed.' }

    $QmtDirName = ([string][char]0x5927) + 'QMT' + ([char]0x5185) + ([char]0x7F6E) + 'python'
    $QmtTests = Join-Path (Join-Path $Root $QmtDirName) 'run_tests.ps1'
    & powershell -NoProfile -ExecutionPolicy Bypass -File $QmtTests -PythonExe $PythonExe
    if ($LASTEXITCODE -ne 0) { throw 'Big QMT embedded package tests failed.' }
    & powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $Root 'generate_helper.ps1') `
        -EnvFile (Join-Path $Root '.env.example') -PythonExe $PythonExe -AllowExample
    if ($LASTEXITCODE -ne 0) { throw 'Root helper generation wrapper failed.' }

    $ApiDirName = ([string][char]0x5916) + ([char]0x7F6E) + ([char]0x7B56) + ([char]0x7565) + 'API'
    $ApiDir = Join-Path $Root $ApiDirName
    Push-Location $ApiDir
    try {
        & $PythonExe -B -m unittest discover -s tests -v
        if ($LASTEXITCODE -ne 0) { throw 'External strategy API tests failed.' }
    }
    finally {
        Pop-Location
    }
    Write-Host 'One-machine Big QMT bridge validation passed.' -ForegroundColor Green
}
finally {
    if ($Pushed) { Pop-Location }
    [Environment]::SetEnvironmentVariable(
        'PYTHONDONTWRITEBYTECODE',
        $PreviousDontWriteBytecode,
        'Process'
    )
    if (Test-Path -LiteralPath $Generated) {
        Remove-Item -LiteralPath $Generated -Recurse -Force
    }
}
