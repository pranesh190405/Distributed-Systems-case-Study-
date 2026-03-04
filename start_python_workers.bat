@echo off
REM Start 3 Python Worker nodes locally for testing

echo Starting Worker 1 (Port 8001, Weight 1)...
start "Worker 8001" cmd /k "python worker.py 8001 1"

echo Starting Worker 2 (Port 8002, Weight 1)...
start "Worker 8002" cmd /k "python worker.py 8002 1"

echo Starting Worker 3 (Port 8003, Weight 1)...
start "Worker 8003" cmd /k "python worker.py 8003 1"

echo All 3 workers started in separate windows!
echo If a window shows an error (like 'python is not recognized'), please take a screenshot!
pause
