@echo off
REM Script to run the Streamlit dashboard on Windows
REM Usage: scripts\run_dashboard.bat [port]

setlocal

REM Get the project root directory
set "PROJECT_ROOT=%~dp0.."
cd /d "%PROJECT_ROOT%"

REM Default port
set "PORT=%1"
if "%PORT%"=="" set "PORT=8501"

REM Check if virtual environment exists
if exist "venv\Scripts\activate.bat" (
    echo Activating virtual environment...
    call venv\Scripts\activate.bat
) else if exist ".venv\Scripts\activate.bat" (
    echo Activating virtual environment...
    call .venv\Scripts\activate.bat
)

REM Check if streamlit is installed
where streamlit >nul 2>&1
if errorlevel 1 (
    echo Streamlit is not installed. Installing dependencies...
    pip install -r requirements.txt
)

REM Check if data directory exists
if not exist "data\output" (
    echo Warning: data\output directory does not exist. Creating it...
    mkdir data\output\dashboard
)

REM Run the dashboard
echo Starting Streamlit dashboard on port %PORT%...
echo Dashboard will be available at: http://localhost:%PORT%
echo Press Ctrl+C to stop the server

streamlit run src\dashboard\app.py --server.port=%PORT% --server.address=localhost

endlocal

