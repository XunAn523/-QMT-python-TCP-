[CmdletBinding()]
param([string]$PythonExe)

$ErrorActionPreference = 'Stop'
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path $Root -Parent
. (Join-Path $ProjectRoot 'tools\Resolve-ProjectPython.ps1')
$PythonExe = Resolve-QmtProjectPython -ProjectRoot $ProjectRoot -PythonExe $PythonExe
$EnvExample = Join-Path $ProjectRoot '.env.example'
$Build = Join-Path ([IO.Path]::GetTempPath()) ('qmt-local-helper-' + [guid]::NewGuid().ToString('N'))
$LocationPushed = $false
try {
    Push-Location $Root
    $LocationPushed = $true
    $SyntaxCheck = @'
import ast
import pathlib
import sys
for raw_path in sys.argv[1:]:
    path = pathlib.Path(raw_path)
    ast.parse(path.read_text(encoding='utf-8-sig'), filename=str(path))
'@
    $SyntaxFiles = @(
        'tools\generate_helpers.py',
        'src\bigqmt_file_queue_helper.py',
        'src\bigqmt_loader.py',
        'tests\test_generator.py',
        'tests\test_helper_runtime.py'
    )
    & $PythonExe -B -c $SyntaxCheck @SyntaxFiles
    if ($LASTEXITCODE -ne 0) { throw "Python AST syntax check failed: $LASTEXITCODE" }
    & $PythonExe -B -m unittest discover -s tests -v
    if ($LASTEXITCODE -ne 0) { throw "Unit tests failed: $LASTEXITCODE" }
    & $PythonExe -B tools\generate_helpers.py --env-file $EnvExample --output $Build --allow-example --ignore-process-env
    if ($LASTEXITCODE -ne 0) { throw "Example generation failed: $LASTEXITCODE" }
    & $PythonExe -B tools\generate_helpers.py --env-file $EnvExample --output $Build --check --allow-example --ignore-process-env
    if ($LASTEXITCODE -ne 0) { throw "Generated helper check failed: $LASTEXITCODE" }
}
finally {
    if ($LocationPushed) { Pop-Location }
    if (Test-Path -LiteralPath $Build) {
        Remove-Item -LiteralPath $Build -Recurse -Force
    }
}

Write-Host 'Local Big QMT embedded-Python package validation passed.' -ForegroundColor Green
