# Reload PhlyData aircraft from internaldb/aircraft.csv into PostgreSQL (public.phlydata_aircraft).
# Requires: Python 3.10+, POSTGRES_CONNECTION_STRING in etl-pipeline/.env or backend/.env
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File D:\HyeAero\etl-pipeline\scripts\run_phlydata_etl.ps1
#   powershell -ExecutionPolicy Bypass -File .\run_phlydata_etl.ps1 --reset
#   powershell -ExecutionPolicy Bypass -File .\run_phlydata_etl.ps1 --dry-run
#   powershell -ExecutionPolicy Bypass -File .\run_phlydata_etl.ps1 --internal-csv "D:\data\aircraft.csv"
#   powershell -ExecutionPolicy Bypass -File .\run_phlydata_etl.ps1 --drop-legacy-csv-extra

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PipelineRoot = Split-Path -Parent $ScriptDir
Set-Location $PipelineRoot

$python = $env:PYTHON
if (-not $python) { $python = "python" }

$pyScript = Join-Path $PipelineRoot "scripts\build_phlydata_aircraft_table.py"
if (-not (Test-Path $pyScript)) {
    Write-Error "Not found: $pyScript"
}

Write-Host "Working directory: $PipelineRoot"
Write-Host "Running: $python `"$pyScript`" $($args -join ' ')"
& $python $pyScript @args
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Write-Host "Done."
