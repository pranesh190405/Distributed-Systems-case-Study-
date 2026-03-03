#!/bin/bash
# ============================================
# Start Worker Nodes on localhost
# ============================================
# Usage: ./start_workers.sh
# This starts 3 worker instances on ports 8001, 8002, 8003

JAR="target/distributed-system-1.0.jar"

if [ ! -f "$JAR" ]; then
    echo "ERROR: JAR not found. Run 'mvn package -DskipTests' first."
    exit 1
fi

echo "Starting Worker 1 on port 8001 (weight=1)..."
java --add-opens java.management/com.sun.management=ALL-UNNAMED \
     -cp "$JAR" com.distributed.worker.WorkerServer 8001 1 &
PID1=$!

echo "Starting Worker 2 on port 8002 (weight=1)..."
java --add-opens java.management/com.sun.management=ALL-UNNAMED \
     -cp "$JAR" com.distributed.worker.WorkerServer 8002 1 &
PID2=$!

echo "Starting Worker 3 on port 8003 (weight=1)..."
java --add-opens java.management/com.sun.management=ALL-UNNAMED \
     -cp "$JAR" com.distributed.worker.WorkerServer 8003 1 &
PID3=$!

echo ""
echo "All 3 workers started!"
echo "  Worker 1: PID=$PID1, port=8001"
echo "  Worker 2: PID=$PID2, port=8002"
echo "  Worker 3: PID=$PID3, port=8003"
echo ""
echo "Press Ctrl+C to stop all workers."

# Wait and cleanup on exit
trap "echo 'Stopping workers...'; kill $PID1 $PID2 $PID3 2>/dev/null; exit 0" SIGINT SIGTERM
wait
