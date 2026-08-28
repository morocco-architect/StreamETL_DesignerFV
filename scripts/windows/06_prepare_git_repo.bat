@echo off
setlocal
cd /d "%~dp0\..\.."

if exist .git (
  echo Git repository already initialized.
) else (
  git init || exit /b 1
)

git add .
git status

echo.
echo Review the staged files, then commit with:
echo   git commit -m "StreamETL 1.2.0-rc1 validated release candidate"
endlocal
