@echo off
setlocal
cd /d "%~dp0\..\.."

docker image inspect streametl-spark-python:3.5.1 >nul 2>&1
if errorlevel 1 (
  echo Local Spark image is missing. Building it first...
  call scripts\windows\00_build_spark_image.bat || exit /b 1
)

cd deploy
docker compose up -d || exit /b 1
docker compose ps
endlocal
