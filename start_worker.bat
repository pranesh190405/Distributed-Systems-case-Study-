@echo off
REM ============================================
REM   WORKER NODE - Run on remote machines
REM ============================================
REM Copy these files to each worker laptop:
REM   - worker.py
REM   - protocol.py
REM   - computation.py
REM   - requirements.txt
REM   - start_worker.bat (this file)
REM
REM Install dependencies: pip install -r requirements.txt
REM Then double-click this file to start!
REM ============================================

set PORT=%1
if "%PORT%"=="" set PORT=8001

set WEIGHT=%2
if "%WEIGHT%"=="" set WEIGHT=1

set THREADS=%3
if "%THREADS%"=="" set THREADS=4

echo ============================================
echo   Distributed Worker Node
echo ============================================
echo   Port:    %PORT%
echo   Weight:  %WEIGHT%
echo   Threads: %THREADS%
echo.

echo This machine's IP addresses:
ipconfig | findstr /i "IPv4"
echo.

echo Worker will auto-broadcast on UDP port 9999.
echo The Master will discover this worker automatically!
echo.

python worker.py %PORT% %WEIGHT% %THREADS%
pause
