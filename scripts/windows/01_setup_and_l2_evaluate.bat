@echo off
setlocal
cd /d "%~dp0\..\.."

where py >nul 2>&1
if errorlevel 1 (
  echo Python launcher 'py' not found. Install Python 3.12 or 3.13 x64 first.
  exit /b 1
)

set "PYVER="
py -3.13 --version >nul 2>&1
if not errorlevel 1 set "PYVER=-3.13"

if not defined PYVER (
  py -3.12 --version >nul 2>&1
  if not errorlevel 1 (
    set "PYVER=-3.12"
    echo Python 3.13 not found; using Python 3.12 for this evaluation run.
  )
)

if not defined PYVER (
  echo Python 3.13 or 3.12 is required.
  exit /b 1
)

if not exist .venv py %PYVER% -m venv .venv
call .venv\Scripts\activate
python -m pip install --upgrade pip
python -m pip install -r backend\requirements-dev.txt
python -m pip install -r evaluation\requirements-eval.txt

if not exist evaluation\results mkdir evaluation\results

cd backend
python -m compileall -q .
if errorlevel 1 exit /b 1
python -m pytest -q --junitxml=..\evaluation\results\pytest.xml --cov=. --cov-config=.coveragerc --cov-report=xml:..\evaluation\results\coverage.xml --cov-report=term-missing
if errorlevel 1 exit /b 1
cd ..

python evaluation\run_l2_evaluation.py --iterations 50
if errorlevel 1 exit /b 1
python evaluation\plot_evaluation.py
if errorlevel 1 exit /b 1

echo.
echo DONE. Results: evaluation\results
echo Figures: evaluation\figures
endlocal
