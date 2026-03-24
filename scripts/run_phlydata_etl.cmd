@echo off
setlocal
REM Reload PhlyData aircraft from internaldb/aircraft.csv into PostgreSQL.
REM Requires POSTGRES_CONNECTION_STRING in etl-pipeline\.env or backend\.env
REM
REM Usage (double-click or from cmd):
REM   D:\HyeAero\etl-pipeline\scripts\run_phlydata_etl.cmd
REM   run_phlydata_etl.cmd --reset
REM   run_phlydata_etl.cmd --dry-run
REM   run_phlydata_etl.cmd --internal-csv "D:\data\aircraft.csv"
REM   run_phlydata_etl.cmd --drop-legacy-csv-extra

set "SCRIPT_DIR=%~dp0"
set "PIPELINE_ROOT=%SCRIPT_DIR%.."
cd /d "%PIPELINE_ROOT%"

if "%PYTHON%"=="" set PYTHON=python
"%PYTHON%" scripts\build_phlydata_aircraft_table.py %*
set "EC=%ERRORLEVEL%"
if not "%EC%"=="0" exit /b %EC%
echo Done.
exit /b 0
