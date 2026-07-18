Set-StrictMode -Version Latest

function Read-QmtLocalEnv {
    param([Parameter(Mandatory = $true)][string]$Path)
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Environment file not found: $Path"
    }
    $Parsed = @{}
    $LineNumber = 0
    foreach ($RawLine in [IO.File]::ReadAllLines($Path, [Text.UTF8Encoding]::new($false, $true))) {
        $LineNumber++
        $Line = $RawLine.Trim()
        if (-not $Line -or $Line.StartsWith('#')) { continue }
        if ($Line.StartsWith('export ')) { throw "Line $LineNumber must not use export" }
        $Equals = $Line.IndexOf('=')
        if ($Equals -lt 1) { throw "Line $LineNumber must use KEY=VALUE" }
        $Key = $Line.Substring(0, $Equals).Trim()
        $Value = $Line.Substring($Equals + 1).Trim()
        if ($Key -notmatch '^QMT_LOCAL_[A-Z0-9_]+$') { throw "Invalid key at line $LineNumber" }
        if ($Parsed.ContainsKey($Key)) { throw "Duplicate key at line $LineNumber" }
        $Quoted = $false
        if ($Value.Length -ge 2) {
            $First = $Value.Substring(0, 1)
            $Last = $Value.Substring($Value.Length - 1, 1)
            if (($First -eq '"' -and $Last -eq '"') -or ($First -eq "'" -and $Last -eq "'")) {
                $Quoted = $true
                $Value = $Value.Substring(1, $Value.Length - 2)
            }
        }
        if (-not $Quoted -and $Value.Contains('#')) { throw "Inline comments are forbidden at line $LineNumber" }
        $Parsed[$Key] = $Value
    }
    return $Parsed
}
