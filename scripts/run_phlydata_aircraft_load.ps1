# Load store/raw/internaldb/aircraft.csv into PostgreSQL public.phlydata_aircraft
# Requires POSTGRES_CONNECTION_STRING in etl-pipeline/.env or backend/.env
#
# Usage (from repo root):
#   .\etl-pipeline\scripts\run_phlydata_aircraft_load.ps1
# Full refresh (truncate then load — use after schema/column changes):
#   .\etl-pipeline\scripts\run_phlydata_aircraft_load.ps1 -Reset

param(
    [switch] $Reset,
    [switch] $DryRun
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
Set-Location (Join-Path $Root "etl-pipeline")

$py = Join-Path $Root "etl-pipeline\venv\Scripts\python.exe"
if (-not (Test-Path $py)) {
    $py = "python"
}

$args = @("scripts\build_phlydata_aircraft_table.py")
if ($Reset) { $args += "--reset" }
if ($DryRun) { $args += "--dry-run" }

& $py @args
exit $LASTEXITCODE
