@echo off
setlocal
cd /d "%~dp0\..\.."

if not exist .venv\Scripts\activate.bat (
  echo ERROR: .venv is missing. Run scripts\windows\01_setup_and_l2_evaluate.bat first.
  exit /b 1
)

call .venv\Scripts\activate
cd backend

if not defined DATABASE_URL set "DATABASE_URL=postgresql://admin:admin@localhost:5433/streametl"
if not defined STREAMETL_AI_PROVIDER set "STREAMETL_AI_PROVIDER=disabled"
if not defined STREAMETL_AI_TIMEOUT_SECONDS set "STREAMETL_AI_TIMEOUT_SECONDS=180"

echo DATABASE_URL=%DATABASE_URL%
echo STREAMETL_AI_PROVIDER=%STREAMETL_AI_PROVIDER%
echo STREAMETL_AI_MODEL=%STREAMETL_AI_MODEL%
echo STREAMETL_AI_TIMEOUT_SECONDS=%STREAMETL_AI_TIMEOUT_SECONDS%

python -m uvicorn main:app --host 127.0.0.1 --port 8000
endlocal
