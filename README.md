# Distributed Scientific Computing — Load Balancer Comparison

A Java distributed system that distributes real scientific computing tasks across worker nodes via TCP sockets, comparing 5 load balancing algorithms.

## 🔬 Task Types

| Task | Description | How It's Distributed |
|---|---|---|
| **Matrix Multiplication** | N×N matrix multiply | Matrix A split into row-slabs across workers |
| **Monte Carlo Pi** | π estimation via random sampling | Total samples divided equally across workers |
| **Prime Factorization** | Factor large numbers (10¹²-10¹⁵) | Number list split into batches per worker |

## ⚖️ Load Balancing Algorithms

| Algorithm | Strategy |
|---|---|
| Round Robin | Cyclic assignment |
| Weighted Round Robin | Proportional to node weight |
| Least Connections | Worker with fewest active tasks |
| Least Response Time | Worker with lowest avg response time |
| Random | Random worker selection (baseline) |

## 🚀 Quick Start

### 1. Build the project
```bash
mvn clean package -DskipTests
```

### 2. Start worker nodes (3 workers on localhost)
```bash
./start_workers.sh
```
Or manually:
```bash
java --add-opens java.management/com.sun.management=ALL-UNNAMED \
     -cp target/distributed-system-1.0.jar \
     com.distributed.worker.WorkerServer 8001 1 &
java --add-opens java.management/com.sun.management=ALL-UNNAMED \
     -cp target/distributed-system-1.0.jar \
     com.distributed.worker.WorkerServer 8002 1 &
java --add-opens java.management/com.sun.management=ALL-UNNAMED \
     -cp target/distributed-system-1.0.jar \
     com.distributed.worker.WorkerServer 8003 1 &
```

### 3. Start the Master Dashboard
```bash
./start_master.sh
```
Or manually:
```bash
java --add-opens java.management/com.sun.management=ALL-UNNAMED \
     -cp target/distributed-system-1.0.jar \
     com.distributed.gui.Launcher
```

### 4. In the Dashboard
1. Verify workers show as 🟢 Online
2. Select a **Task Type** (e.g., Monte Carlo Pi)
3. Select a **Load Balancing Algorithm** (e.g., Round Robin)
4. Click **▶ Run Experiment**
5. Or click **▶▶ Run All 15 Experiments** for the full comparison

## 📁 Project Structure

```
src/main/java/com/distributed/
├── model/           # TaskType, TaskChunk, TaskResult, NodeInfo, MetricSnapshot
├── computation/     # MatrixEngine, MonteCarloEngine, PrimeFactorizationEngine
├── worker/          # WorkerServer (TCP listener + thread pool)
├── master/          # MasterServer, TaskSplitter, ResultAssembler, TaskDispatcher
├── balancer/        # LoadBalancer interface + 5 implementations
├── metrics/         # MetricsCollector (heartbeats + metrics aggregation)
├── network/         # NetworkProtocol (TCP serialization), HeartbeatMessage
└── gui/             # DashboardApp (JavaFX), Launcher
```

## ⚙️ Configuration

Edit `config.properties` to adjust:

```properties
# Workers (host:port:weight)
workers=localhost:8001:1,localhost:8002:1,localhost:8003:1

# Task parameters
matrix.size=500              # N×N matrix size
montecarlo.samples=10000000  # Total Monte Carlo samples
prime.count=500              # Numbers to factorize
chunks.count=12              # Work chunks (for load balancing granularity)
```

## 🌐 Multi-Device Setup

To run on multiple laptops:
1. Start `WorkerServer` on each laptop (one per device)
2. Update `config.properties` with each laptop's IP:
   ```
   workers=192.168.1.10:8001:1,192.168.1.11:8001:2,192.168.1.12:8001:1
   ```
3. Assign different weights to reflect machine capabilities
4. Start the Dashboard on the master machine

## 📊 Comparison Output

After running all 15 experiments (3 tasks × 5 algorithms), the comparison table shows:

| Metric | Description |
|---|---|
| Total Time (ms) | End-to-end experiment duration |
| Throughput | Tasks completed per second |
| Load Std Dev | Standard deviation of task distribution (lower = more balanced) |
| Max CPU % | Peak worker CPU utilization |
| Result Summary | Computed answer (π value, matrix verification, factorization count) |

## Requirements

- Java 17+ (tested with Java 21)
- Maven 3.6+
