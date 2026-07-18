$ErrorActionPreference = 'Stop'
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
. (Join-Path $Root 'tools\Resolve-ProjectPython.ps1')
$EnvFile = Join-Path $Root '.env.example'
$Original = [Environment]::GetEnvironmentVariable('QMT_LOCAL_TCP_PORT', 'Process')
$OriginalBind = [Environment]::GetEnvironmentVariable('QMT_LOCAL_BIND_HOST', 'Process')
$OriginalAuth = [Environment]::GetEnvironmentVariable('QMT_LOCAL_AUTH_TOKEN', 'Process')
$InvalidFile = Join-Path ([IO.Path]::GetTempPath()) ('qmt-local-invalid-env-' + [guid]::NewGuid().ToString('N'))
try {
    [Environment]::SetEnvironmentVariable('QMT_LOCAL_TCP_PORT', '10550', 'Process')
    [Environment]::SetEnvironmentVariable('QMT_LOCAL_BIND_HOST', $null, 'Process')
    [Environment]::SetEnvironmentVariable('QMT_LOCAL_AUTH_TOKEN', $null, 'Process')
    $Parsed = Read-QmtLocalEnv -Path $EnvFile
    if ([string]$Parsed['QMT_LOCAL_TCP_PORT'] -ne '9550') { throw 'File parse returned the wrong value.' }
    if ($env:QMT_LOCAL_TCP_PORT -ne '10550') { throw 'Read-QmtLocalEnv mutated the process environment.' }
    if ($null -ne [Environment]::GetEnvironmentVariable('QMT_LOCAL_AUTH_TOKEN', 'Process')) {
        throw 'Read-QmtLocalEnv leaked the authentication token.'
    }
    if ($env:QMT_LOCAL_TCP_PORT -ne '10550') { throw 'Existing environment was mutated.' }
    if ($null -ne [Environment]::GetEnvironmentVariable('QMT_LOCAL_BIND_HOST', 'Process')) {
        throw 'Parser injected an environment value.'
    }
    [IO.File]::WriteAllLines(
        $InvalidFile,
        @('QMT_LOCAL_BIND_HOST=127.0.0.1', 'this line is invalid'),
        [Text.UTF8Encoding]::new($false)
    )
    $Rejected = $false
    try { Read-QmtLocalEnv -Path $InvalidFile | Out-Null }
    catch { $Rejected = $true }
    if (-not $Rejected) { throw 'Invalid env file was accepted.' }
    if ($null -ne [Environment]::GetEnvironmentVariable('QMT_LOCAL_BIND_HOST', 'Process')) {
        throw 'A parse failure leaked a partially imported environment.'
    }

    $ExpectedPython = [IO.Path]::GetFullPath((Join-Path $Root '.venv\Scripts\python.exe'))
    $ResolvedDefault = Resolve-QmtProjectPython -ProjectRoot $Root -EnvFile $EnvFile
    if (-not $ResolvedDefault.Equals($ExpectedPython, [StringComparison]::OrdinalIgnoreCase)) {
        throw 'The default project Python did not resolve to .venv.'
    }
    $ResolvedExplicit = Resolve-QmtProjectPython `
        -ProjectRoot $Root `
        -PythonExe '.venv\Scripts\python.exe'
    if (-not $ResolvedExplicit.Equals($ExpectedPython, [StringComparison]::OrdinalIgnoreCase)) {
        throw 'The explicit project Python did not resolve to .venv.'
    }
    $ForeignRejected = $false
    try {
        Resolve-QmtProjectPython -ProjectRoot $Root -PythonExe 'tools\project_env.py' | Out-Null
    }
    catch {
        if ($_.Exception.Message -notlike '*must resolve to this project*') { throw }
        $ForeignRejected = $true
    }
    if (-not $ForeignRejected) { throw 'A project-external Python override was accepted.' }
}
finally {
    [Environment]::SetEnvironmentVariable('QMT_LOCAL_TCP_PORT', $Original, 'Process')
    [Environment]::SetEnvironmentVariable('QMT_LOCAL_BIND_HOST', $OriginalBind, 'Process')
    [Environment]::SetEnvironmentVariable('QMT_LOCAL_AUTH_TOKEN', $OriginalAuth, 'Process')
    if (Test-Path -LiteralPath $InvalidFile) { Remove-Item -LiteralPath $InvalidFile -Force }
}
Write-Host 'PowerShell project .env parser/no-leak/.venv contract passed.' -ForegroundColor Green
