@echo off
setlocal
cd /d "%~dp0\..\..\frontend"
npm ci
if errorlevel 1 exit /b 1
npm run dev
endlocal
