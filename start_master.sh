#!/bin/bash
# ============================================
# Start Master Dashboard (JavaFX GUI)
# ============================================
# Usage: ./start_master.sh
# Make sure workers are running first!

JAR="target/distributed-system-1.0.jar"

if [ ! -f "$JAR" ]; then
    echo "ERROR: JAR not found. Run 'mvn package -DskipTests' first."
    exit 1
fi

echo "Starting Master Dashboard..."
echo "Make sure workers are already running (./start_workers.sh)"
echo ""

java --add-opens java.management/com.sun.management=ALL-UNNAMED \
     -cp "$JAR" com.distributed.gui.Launcher
