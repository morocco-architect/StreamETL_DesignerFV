@echo off
setlocal
cd /d "%~dp0\..\..\frontend"

echo ============================================
echo StreamETL Designer - Frontend Build
echo ============================================

node --version || exit /b 1
call npm --version || exit /b 1

if not exist node_modules (
  echo Installing dependencies with npm ci...
  call npm ci || exit /b 1
)

echo Building production frontend...
call npm run build || exit /b 1

echo.
echo BUILD SUCCESSFUL
echo Output: %CD%\dist
endlocal
