@echo off
setlocal
cd /d "%~dp0"

where python >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Python not found in PATH.
  echo Install Python 3.10+ and add it to PATH, then retry.
  pause
  exit /b 1
)

echo Starting bettips...
python -c "import apscheduler, loguru, yaml, requests" >nul 2>nul
if errorlevel 1 (
  echo Installing dependencies...
  python -m pip install -r requirements.txt
  if errorlevel 1 (
    echo [ERROR] Failed to install dependencies.
    pause
    exit /b 1
  )
)

python main.py
set EXIT_CODE=%ERRORLEVEL%

if not "%EXIT_CODE%"=="0" (
  echo.
  echo [ERROR] main.py exited with code %EXIT_CODE%.
)
pause
exit /b %EXIT_CODE%
