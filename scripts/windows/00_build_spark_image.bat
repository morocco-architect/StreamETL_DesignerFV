@echo off
setlocal
cd /d "%~dp0\..\.."

echo Building Apache Spark 3.5.1 base image...
docker build -t spark:3.5.1-scala2.12-java11-ubuntu deploy\spark-image\base || exit /b 1

echo Building StreamETL Spark Python image...
docker build -t streametl-spark-python:3.5.1 deploy\spark-image\python || exit /b 1

echo Spark image ready: streametl-spark-python:3.5.1
endlocal
