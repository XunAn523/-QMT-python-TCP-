Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot 'Load-ProjectEnv.ps1')

function Resolve-QmtProjectPython {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$ProjectRoot,
        [string]$EnvFile,
        [string]$PythonExe
    )

    $Root = [IO.Path]::GetFullPath($ProjectRoot)
    $Expected = [IO.Path]::GetFullPath(
        (Join-Path $Root '.venv\Scripts\python.exe')
    )

    if ($PythonExe) {
        if ([IO.Path]::IsPathRooted($PythonExe)) {
            $Candidate = [IO.Path]::GetFullPath($PythonExe)
        }
        elseif ($PythonExe.Contains('\') -or $PythonExe.Contains('/')) {
            $Candidate = [IO.Path]::GetFullPath((Join-Path $Root $PythonExe))
        }
        else {
            $Command = Get-Command -Name $PythonExe -CommandType Application -ErrorAction Stop
            $Candidate = [IO.Path]::GetFullPath($Command.Source)
        }
    }
    else {
        if ($EnvFile) {
            $Values = Read-QmtLocalEnv -Path $EnvFile
            $Configured = [string]$Values['QMT_LOCAL_PYTHON_EXE']
            if (-not $Configured -or [IO.Path]::IsPathRooted($Configured)) {
                throw 'QMT_LOCAL_PYTHON_EXE must remain .venv\Scripts\python.exe'
            }
            $ConfiguredPath = [IO.Path]::GetFullPath((Join-Path $Root $Configured))
            if (-not $ConfiguredPath.Equals($Expected, [StringComparison]::OrdinalIgnoreCase)) {
                throw 'QMT_LOCAL_PYTHON_EXE must remain .venv\Scripts\python.exe'
            }
        }
        $Candidate = $Expected
    }

    if (-not $Candidate.Equals($Expected, [StringComparison]::OrdinalIgnoreCase)) {
        throw 'PythonExe must resolve to this project''s .venv\Scripts\python.exe'
    }
    if (-not (Test-Path -LiteralPath $Candidate -PathType Leaf)) {
        throw "Project virtual-environment Python was not found: $Candidate. Run .\setup_venv.ps1 first."
    }
    return $Candidate
}
