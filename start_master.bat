@echo off
REM ============================================
REM   MASTER NODE - Run on YOUR laptop
REM ============================================
REM Make sure workers on other laptops are running first!
REM Workers will be auto-discovered via UDP broadcast.
REM
REM Install dependencies: pip install -r requirements.txt
REM ============================================

echo ============================================
echo   Starting Master Dashboard...
echo   Dashboard will open at http://localhost:5000
echo ============================================
echo.

python master.py

echo.
echo If you see an error above (like 'python is not recognized'),
echo please install Python and try again.
pause
