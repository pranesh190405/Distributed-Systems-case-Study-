# 🖧 Distributed Scientific Computing — Case Study

A **multi-machine distributed computing system** built from scratch in Python. A central **Master** node splits scientific tasks into chunks and dispatches them to networked **Worker** nodes. The system compares **5 load-balancing algorithms** across **9 computation tasks** (benchmarks + real-world use cases) through a live **Flask web dashboard**.

---

## Table of Contents

- [System Architecture](#system-architecture)
- [Features](#features)
- [Project Structure](#project-structure)
- [Computation Tasks](#computation-tasks)
- [Load Balancing Algorithms](#load-balancing-algorithms)
- [Requirements](#requirements)
- [Setup & Running](#setup--running)
  - [Master Node](#master-node-your-machine)
  - [Worker Node (Other Machines)](#worker-node-other-machines)
- [Configuration](#configuration)
- [Network Protocol](#network-protocol)
- [Data Models](#data-models)
- [Web Dashboard](#web-dashboard)

---

## System Architecture

```
┌─────────────────────────────────────────┐
│              MASTER NODE                │
│  ┌──────────┐   ┌────────────────────┐  │
│  │  Flask   │   │   MasterServer     │  │
│  │Dashboard │──▶│  - Task Splitting  │  │
│  └──────────┘   │  - Load Balancing  │  │
│                 │  - Result Assembly │  │
│                 └────────┬───────────┘  │
└──────────────────────────│──────────────┘
                           │ TCP (JSON + length-prefix framing)
           ┌───────────────┼───────────────┐
           ▼               ▼               ▼
    ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
    │  Worker A   │ │  Worker B   │ │  Worker C   │
    │ worker.py   │ │ worker.py   │ │ worker.py   │
    │ :8001 w=2   │ │ :8001 w=1   │ │ :8001 w=1   │
    └─────────────┘ └─────────────┘ └─────────────┘
           ▲               ▲
           └───────────────┘
             UDP Broadcast (auto-discovery, port 9999)
```

**Flow:**
1. Master splits a task into N chunks
2. Load balancer selects a worker for each chunk
3. Worker computes the chunk and returns the result via TCP
4. Master assembles all results and records metrics

---

## Features

-  **Multi-machine deployment** over a local Wi-Fi network
-  **5 load balancing algorithms** compared side-by-side
-  **9 computation task types** (4 benchmarks + 5 real-world use cases)
-  **Automatic worker discovery** via UDP broadcast
-  **Live heartbeat monitoring** — detects worker failures in real time
-  **Automatic chunk re-queuing** on worker failure
-  **Flask web dashboard** with real-time metrics, charts, and logs
-  **Configurable** via `config.ini` (no code changes needed)

---

## Project Structure

```
Distributed-Systems-case-Study-/
│
├── master.py          # Master orchestration server + Flask dashboard
├── worker.py          # Worker node TCP server
├── computation.py     # All 9 computation engines
├── balancers.py       # 5 load balancing algorithm implementations
├── protocol.py        # TCP network protocol (JSON + length-prefix framing)
├── models.py          # Data models: TaskChunk, TaskResult, NodeInfo, MetricSnapshot
│
├── config.ini         # Configuration: workers, task parameters, timeouts
├── requirements.txt   # Python dependencies
├── instructions.txt   # Step-by-step worker setup guide
│
├── templates/
│   └── dashboard.html # Flask web UI (real-time monitoring dashboard)
│
└── datasets/
    └── accesslog.csv  # Web server log dataset (used by Log Analysis task)
```

---

## Computation Tasks

The system supports 9 task types, split into **Benchmarks** (pure CPU tests) and **Use Cases** (real-world simulations):

### Benchmarks

| Task | Description | Parallelism Strategy |
|------|-------------|----------------------|
| **Matrix Multiplication** | Parallel row-slab multiply of an N×N matrix | Each chunk = a horizontal slab of rows |
| **Monte Carlo Pi** | Distributed random sampling to estimate π | Each chunk = a subset of random samples |
| **Prime Factorization** | Trial division on batches of large numbers | Each chunk = a batch of numbers |
| **Data Sorting** | Sorting large arrays of random floats | Each chunk = an independent 100k-element array |

### Use Cases

| Task | Description | Key Insight |
|------|-------------|-------------|
| **RSA Key Cracking** | Brute-force factorize large semiprimes (skewed workload) | Demonstrates dynamic balancers handling uneven tasks |
| **ETL Log Aggregation** | Parse, filter, and sort 50k JSON server logs per chunk | Memory-bound, I/O-intensive workload |
| **Image Blur** | Apply Gaussian blur (radius=15) to generated images | CPU-bound, tests Pillow availability on workers |
| **Crypto Proof-of-Work** | Find SHA-256 nonce with N leading zeros | Simulates blockchain mining difficulty |
| **Web Server Log Analysis** | Distributed parsing of `accesslog.csv` | Aggregates IP hits, status codes, bandwidth |

---

## Load Balancing Algorithms

All 5 algorithms are defined in `balancers.py` and implement the same `LoadBalancer` interface.

| Algorithm | Strategy | Best For |
|-----------|----------|----------|
| **Round Robin** | Cyclic assignment, ignores actual load | Equal-duration tasks |
| **Weighted Round Robin** | Proportional to each node's `weight` setting | Heterogeneous machines |
| **Least Connections** | Routes to worker with fewest active tasks | Variable-duration tasks |
| **Least Response Time** | Routes to worker with lowest avg response time; falls back to Least Connections on cold start | Adaptive, best all-around |
| **Random** | Random selection – baseline for comparison | None (baseline only) |

---

## Requirements

- Python 3.9+
- All machines on the **same Wi-Fi (LAN) network**

Install dependencies:

```bash
pip install -r requirements.txt
```

**`requirements.txt`:**
```
Flask==3.0.0
psutil==5.9.6
numpy>=1.26.2
```

> **Note:** The `Image Blur` use case additionally requires `Pillow`. Install it on worker machines if you plan to run that task:
> ```bash
> pip install Pillow
> ```

---

## Setup & Running

> **Important:** Always start Workers **before** starting the Master.

### Worker Node (Other Machines)

**Files needed on each worker machine:**
- `worker.py`
- `protocol.py`
- `computation.py`
- `requirements.txt`

**Step 1 — Install Python**
Download from [python.org](https://python.org). Check **"Add Python to PATH"** during installation.

**Step 2 — Install dependencies**
```powershell
pip install -r requirements.txt
```

**Step 3 — Start the worker**
```powershell
python worker.py [port] [weight] [threads]

# Examples:
python worker.py 8001          # port=8001, weight=1, threads=auto
python worker.py 8001 2        # weight=2 (gets twice as many tasks in WRR)
python worker.py 8001 1 8      # 8 concurrent threads
```

The worker will print its **LAN IP address** — note it for the next step.

**Step 4 — Allow Firewall (Windows)**
If prompted by Windows Firewall, click **Allow Access**. Or run manually:
```powershell
netsh advfirewall firewall add rule name="Worker TCP Listen" dir=in action=allow protocol=TCP localport=8001
netsh advfirewall firewall add rule name="Worker UDP Broadcast" dir=out action=allow protocol=UDP remoteport=9999
```

---

### Master Node (Your Machine)

**Step 1 — Update `config.ini`** with the IP address of each worker:
```ini
[workers]
nodes=10.227.169.136:8001:1
# Multiple workers (comma-separated):
# nodes=192.168.1.100:8001:1,192.168.1.105:8001:1
```
Format: `host:port:weight`

**Step 2 — Start the master server**
```powershell
python master.py
```

**Step 3 — Open the dashboard**  
Navigate to **http://localhost:5000** in your browser.

> Workers can also be discovered **automatically** via UDP broadcast (port 9999) — no `config.ini` change needed if workers are on the same subnet.

---

## Configuration

All parameters are in `config.ini`:

```ini
[workers]
# Comma-separated list of worker nodes: host:port:weight
nodes=10.227.169.136:8001:1

[tasks]
matrix_size=300           # Dimension of N×N matrix
monte_carlo_samples=5000000  # Total random samples for Pi estimation
prime_count=200           # Number of integers to factorize
prime_min=100000          # Min random number for factorization
prime_max=99999999        # Max random number for factorization
chunks_count=12           # Number of chunks to split tasks into

[network]
connect_timeout_ms=10000  # Worker connection timeout (ms)
read_timeout_ms=120000    # Task result read timeout (ms)
heartbeat_timeout_ms=5000 # Heartbeat check timeout (ms)
```

---

## Network Protocol

Defined in `protocol.py`. All communication uses **JSON over TCP with 4-byte length-prefix framing**:

```
┌──────────────────┬──────────────────────────────┐
│  4-byte length   │   JSON payload (UTF-8)        │
│  (big-endian)    │                               │
└──────────────────┴──────────────────────────────┘
```

**Message types:**

| Direction | Type | Description |
|-----------|------|-------------|
| Master → Worker | `task` | Task chunk payload with `task_type` and `data` |
| Master → Worker | `__HEARTBEAT_PING__` | Liveness probe |
| Worker → Master | `task_result` | Computed result + timing info |
| Worker → Master | `heartbeat_response` | CPU/memory stats + active task count |
| Worker → Master | UDP `WORKER_READY:<port>` | Auto-discovery broadcast every 2 seconds |

---

## Data Models

Defined in `models.py`:

| Class | Purpose |
|-------|---------|
| `TaskChunk` | A unit of work sent from Master to Worker. Contains `task_id`, `chunk_id`, `task_type`, and `data`. |
| `TaskResult` | The result of one computed chunk. Includes `success`, `data`, `execution_time_ms`, and `network_latency_ms`. |
| `NodeInfo` | Tracks state of a worker node: liveness, active tasks, completed tasks, avg response time, CPU/memory. |
| `MetricSnapshot` | Captures experiment results: total time, per-worker task distribution, throughput, load std deviation, network latency. |

---

## Web Dashboard

The Flask dashboard (`templates/dashboard.html`) provides:

-  **Worker Status Panel** — live heartbeat, CPU/memory of each node
-  **Run Experiments** — select any task + algorithm and run single or batch experiments
-  **Comparison Charts** — bar charts comparing throughput, total time, and load balance across algorithms
-  **Experiment Log** — real-time log stream from the master
-  **Results Table** — metrics for every experiment run (sortable)

Access at: **http://localhost:5000**

---

## Key Design Decisions

- **No external message queue** — raw TCP sockets with custom framing keep the system dependency-light
- **Thread-per-task dispatching** — `ThreadPoolExecutor` on the master handles concurrency without blocking the Flask API
- **Automatic re-queuing** — if a worker fails mid-task, the chunk is resubmitted to the next available worker
- **UDP auto-discovery** — workers broadcast their presence every 2 seconds, so new nodes join without config changes
- **Configurable weights** — each worker has a `weight` parameter used by the Weighted Round Robin balancer to model heterogeneous hardware

---

*Built as a Distributed Systems case study to empirically compare load balancing strategies across diverse computational workloads.*
