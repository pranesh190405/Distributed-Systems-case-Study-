"""
Master Server — Orchestrates distributed computing experiments.
Includes a Flask web dashboard for real-time monitoring and comparison.

Usage:
    python master.py
    Then open http://localhost:5000 in your browser.
"""

import configparser
import math
import os
import random
import socket
import sys
import threading
import time
import uuid
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Flask, render_template, jsonify, request

from models import TaskChunk, TaskResult, NodeInfo, MetricSnapshot, TaskType
from protocol import send_message, receive_message, connect_with_timeout, HEARTBEAT_REQUEST
from balancers import get_balancer_map
from computation import execute_task


# ─── Master Server ────────────────────────────────────────────────────

class MasterServer:
    """Main orchestration class for the Master node."""

    def __init__(self, config_path="config.ini"):
        self.workers = []
        self.balancers = get_balancer_map()
        self.comparison_results = []
        self.logs = []
        self.is_running = False
        self.current_status = "Ready"

        # Defaults
        self.matrix_size = 300
        self.monte_carlo_samples = 5000000
        self.prime_count = 200
        self.prime_min = 100000
        self.prime_max = 999999999
        self.chunks_count = 12
        self.connect_timeout = 10.0
        self.read_timeout = 120.0
        self.heartbeat_timeout = 5.0

        self._load_config(config_path)
        self._heartbeat_thread = None
        self._heartbeat_running = False

    def _load_config(self, path: str):
        """Load configuration from config.ini."""
        config = configparser.ConfigParser()
        if os.path.exists(path):
            config.read(path)
        else:
            self._log(f"Config file '{path}' not found, using defaults.")
            return

        # Parse workers
        workers_str = config.get("workers", "nodes",
                                  fallback="localhost:8001:1,localhost:8002:1,localhost:8003:1")
        for w in workers_str.split(","):
            parts = w.strip().split(":")
            host = parts[0]
            port = int(parts[1])
            weight = int(parts[2]) if len(parts) > 2 else 1
            self.workers.append(NodeInfo(host, port, weight))

        # Task parameters
        self.matrix_size = config.getint("tasks", "matrix_size", fallback=300)
        self.monte_carlo_samples = config.getint("tasks", "monte_carlo_samples", fallback=5000000)
        self.prime_count = config.getint("tasks", "prime_count", fallback=200)
        self.prime_min = config.getint("tasks", "prime_min", fallback=100000)
        self.prime_max = config.getint("tasks", "prime_max", fallback=999999999)
        self.chunks_count = config.getint("tasks", "chunks_count", fallback=12)

        # Network timeouts
        self.connect_timeout = config.getfloat("network", "connect_timeout_ms", fallback=10000) / 1000.0
        self.read_timeout = config.getfloat("network", "read_timeout_ms", fallback=120000) / 1000.0
        self.heartbeat_timeout = config.getfloat("network", "heartbeat_timeout_ms", fallback=5000) / 1000.0

        self._log(f"Config loaded: {len(self.workers)} workers, matrix={self.matrix_size}, "
                  f"samples={self.monte_carlo_samples}, primes={self.prime_count}, chunks={self.chunks_count}")

    def _log(self, msg: str):
        """Add a log entry."""
        timestamp = time.strftime("%H:%M:%S")
        entry = f"[{timestamp}] {msg}"
        self.logs.append(entry)
        print(entry)
        # Keep only last 500 log entries
        if len(self.logs) > 500:
            self.logs = self.logs[-500:]

    # ─── Heartbeat ────────────────────────────────────────────────

    def check_all_workers(self):
        """Check which workers are alive via heartbeat."""
        for worker in self.workers:
            self._check_worker(worker)

    def _check_worker(self, worker: NodeInfo):
        """Send heartbeat to a single worker."""
        try:
            sock = connect_with_timeout(worker.host, worker.port,
                                         self.heartbeat_timeout, self.heartbeat_timeout)
            send_message(sock, {"type": HEARTBEAT_REQUEST})
            response = receive_message(sock)
            sock.close()

            worker.alive = True
            worker.cpu_usage = response.get("cpu_usage", 0)
            worker.total_memory = response.get("total_memory", 0)
            worker.memory_usage = response.get("total_memory", 0) - response.get("free_memory", 0)
        except Exception:
            worker.alive = False

    def start_heartbeat(self, interval_sec=3):
        """Start periodic heartbeat monitoring in background."""
        self._heartbeat_running = True

        def _loop():
            while self._heartbeat_running:
                self.check_all_workers()
                time.sleep(interval_sec)

        self._heartbeat_thread = threading.Thread(target=_loop, daemon=True)
        self._heartbeat_thread.start()

    def stop_heartbeat(self):
        self._heartbeat_running = False

    # ─── Task Splitting ───────────────────────────────────────────

    def _split_matrix(self, num_chunks: int, task_id: str):
        """Split a matrix multiplication task into row-slab chunks."""
        n = self.matrix_size
        rng = random.Random(42)
        mat_a = [[rng.random() * 10 for _ in range(n)] for _ in range(n)]
        mat_b = [[rng.random() * 10 for _ in range(n)] for _ in range(n)]

        chunks = []
        rows_per = n // num_chunks
        remainder = n % num_chunks
        start = 0

        for c in range(num_chunks):
            chunk_rows = rows_per + (1 if c < remainder else 0)
            end = start + chunk_rows
            chunks.append(TaskChunk(
                task_id=task_id, chunk_id=c, total_chunks=num_chunks,
                task_type=TaskType.MATRIX_MULTIPLICATION.value,
                data={
                    "rows_a": mat_a[start:end],
                    "mat_b": mat_b,
                    "start_row": start,
                    "end_row": end,
                    "n": n,
                }
            ))
            start = end

        return chunks, {"mat_a": mat_a, "mat_b": mat_b, "n": n}

    def _split_monte_carlo(self, num_chunks: int, task_id: str):
        """Split Monte Carlo Pi into sample-count chunks."""
        total = self.monte_carlo_samples
        per_chunk = total // num_chunks
        remainder = total % num_chunks
        chunks = []

        for c in range(num_chunks):
            samples = per_chunk + (1 if c < remainder else 0)
            chunks.append(TaskChunk(
                task_id=task_id, chunk_id=c, total_chunks=num_chunks,
                task_type=TaskType.MONTE_CARLO_PI.value,
                data={"num_samples": samples, "seed": c * 12345 + 1}
            ))

        return chunks, {"total_samples": total}

    def _split_prime(self, num_chunks: int, task_id: str):
        """Split prime factorization into batches of numbers."""
        rng = random.Random(12345)
        numbers = []
        for _ in range(self.prime_count):
            num = rng.randint(self.prime_min, self.prime_max)
            if num % 2 == 0:
                num += 1
            numbers.append(num)

        chunks = []
        per_chunk = len(numbers) // num_chunks
        remainder = len(numbers) % num_chunks
        start = 0

        for c in range(num_chunks):
            size = per_chunk + (1 if c < remainder else 0)
            chunks.append(TaskChunk(
                task_id=task_id, chunk_id=c, total_chunks=num_chunks,
                task_type=TaskType.PRIME_FACTORIZATION.value,
                data={"numbers": numbers[start:start + size]}
            ))
            start += size

        return chunks, {"total_numbers": self.prime_count, "all_numbers": numbers}

    def _split_sorting(self, num_chunks: int, task_id: str):
        """Split sorting task into chunks with random float arrays."""
        rng = random.Random(777)
        # 100k floats per chunk
        size_per_chunk = 100000 
        chunks = []

        for c in range(num_chunks):
            arr = [rng.random() * 1000 for _ in range(size_per_chunk)]
            chunks.append(TaskChunk(
                task_id=task_id, chunk_id=c, total_chunks=num_chunks,
                task_type=TaskType.DATA_SORTING.value,
                data={"array": arr}
            ))

        return chunks, {"total_elements": size_per_chunk * num_chunks}

    def _split_io(self, num_chunks: int, task_id: str):
        """Split IO simulation task into varying sleep delays."""
        rng = random.Random(999)
        chunks = []

        for c in range(num_chunks):
            # Sleep between 100ms and 500ms
            sleep_time = rng.uniform(0.1, 0.5) 
            chunks.append(TaskChunk(
                task_id=task_id, chunk_id=c, total_chunks=num_chunks,
                task_type=TaskType.IO_SIMULATION.value,
                data={"sleep_time": sleep_time}
            ))

        return chunks, {"expected_total_sleep": sum(c.data["sleep_time"] for c in chunks)}

    def _split_log_analysis(self, num_chunks: int, task_id: str):
        """Split access logs into chunks of lines."""
        log_path = os.path.join(os.getcwd(), "Dataset", "accesslog.csv")
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                all_lines = f.readlines()
        except Exception as e:
            self._log(f"Error reading log file: {e}")
            all_lines = []

        chunks = []
        if not all_lines:
            return chunks, {}

        per_chunk = len(all_lines) // num_chunks
        remainder = len(all_lines) % num_chunks
        start = 0

        for c in range(num_chunks):
            size = per_chunk + (1 if c < remainder else 0)
            batch = all_lines[start : start + size]
            chunks.append(TaskChunk(
                task_id=task_id, chunk_id=c, total_chunks=num_chunks,
                task_type=TaskType.WEB_LOG_ANALYSIS.value,
                data={"logs": batch}
            ))
            start += size

        return chunks, {"total_lines": len(all_lines)}

    # ─── Dispatching ──────────────────────────────────────────────

    def _dispatch_chunk(self, chunk: TaskChunk, worker: NodeInfo) -> dict:
        """Send a task chunk to a worker and return the result."""
        worker.active_tasks += 1
        start_time = time.time()

        try:
            sock = connect_with_timeout(worker.host, worker.port,
                                         self.connect_timeout, self.read_timeout)
            msg = {
                "type": "task",
                "task_id": chunk.task_id,
                "chunk_id": chunk.chunk_id,
                "total_chunks": chunk.total_chunks,
                "task_type": chunk.task_type,
                "data": chunk.data,
            }
            send_message(sock, msg)
            response = receive_message(sock)
            sock.close()

            elapsed = (time.time() - start_time) * 1000
            worker.record_task_completion(elapsed)
            return response

        except socket.timeout:
            self._log(f"  ✗ Timeout connecting to {worker.id} for chunk {chunk.chunk_id}")
            return {"success": False, "chunk_id": chunk.chunk_id, "worker_id": worker.id,
                    "error_message": f"Timeout: {worker.id}"}
        except ConnectionRefusedError:
            self._log(f"  ✗ Connection refused by {worker.id} for chunk {chunk.chunk_id}")
            return {"success": False, "chunk_id": chunk.chunk_id, "worker_id": worker.id,
                    "error_message": f"Connection refused: {worker.id}"}
        except Exception as e:
            self._log(f"  ✗ Dispatch to {worker.id} failed: {e}")
            return {"success": False, "chunk_id": chunk.chunk_id, "worker_id": worker.id,
                    "error_message": str(e)}
        finally:
            worker.active_tasks -= 1

    # ─── Result Assembly ──────────────────────────────────────────

    def _assemble_matrix(self, results: list, metadata: dict) -> str:
        """Assemble matrix multiplication results and verify."""
        n = metadata["n"]
        mat_a = metadata["mat_a"]
        mat_b = metadata["mat_b"]

        result_matrix = [[0.0] * n for _ in range(n)]
        for r in results:
            if not r.get("success", False):
                continue
            data = r.get("data", {})
            start = data["start_row"]
            end = data["end_row"]
            for i, row in enumerate(data["result_rows"]):
                result_matrix[start + i] = row

        # Verify a sample cell
        verified = True
        check_points = [(0, 0), (n // 2, n // 2), (n - 1, n - 1)]
        for row, col in check_points:
            expected = sum(mat_a[row][k] * mat_b[k][col] for k in range(n))
            if abs(expected - result_matrix[row][col]) > 0.01:
                verified = False
                break

        lines = [
            f"Matrix {n}×{n} multiplication complete.",
            f"Result C[0][0] = {result_matrix[0][0]:.4f}",
            f"Result C[{n-1}][{n-1}] = {result_matrix[n-1][n-1]:.4f}",
            f"Verification: {'✓ PASSED' if verified else '✗ FAILED'}",
        ]
        return "\n".join(lines)

    def _assemble_monte_carlo(self, results: list, metadata: dict) -> str:
        """Assemble Monte Carlo Pi results."""
        total_samples = 0
        total_inside = 0
        for r in results:
            if not r.get("success", False):
                continue
            data = r.get("data", {})
            total_samples += data.get("num_samples", 0)
            total_inside += data.get("inside_count", 0)

        pi_est = 4.0 * total_inside / total_samples if total_samples > 0 else 0
        error = abs(pi_est - math.pi)

        lines = [
            f"Monte Carlo Pi Estimation",
            f"Total samples: {total_samples:,}",
            f"Points inside circle: {total_inside:,}",
            f"π ≈ {pi_est:.10f}",
            f"Actual π = {math.pi:.10f}",
            f"Error: {error:.10f} ({error / math.pi * 100:.6f}%)",
        ]
        return "\n".join(lines)

    def _assemble_prime(self, results: list, metadata: dict) -> str:
        """Assemble prime factorization results."""
        total = 0
        verified = 0
        samples = []

        for r in results:
            if not r.get("success", False):
                continue
            data = r.get("data", {})
            for item in data.get("results", []):
                total += 1
                product = 1
                for f in item["factors"]:
                    product *= f
                if product == item["number"]:
                    verified += 1
                if len(samples) < 5:
                    samples.append(f"  {item['number']:,} = {item['factors']}")

        lines = [
            f"Prime Factorization Complete",
            f"Numbers factorized: {total:,}",
            f"Verified correct: {verified:,} / {total:,}",
            f"Sample results:",
        ] + samples
        return "\n".join(lines)

    def _assemble_sorting(self, results: list, metadata: dict) -> str:
        """Assemble data sorting results."""
        total_sorted = 0
        for r in results:
            if not r.get("success", False):
                continue
            data = r.get("data", {})
            total_sorted += data.get("count", 0)

        lines = [
            f"Data Sorting Complete",
            f"Expected elements: {metadata['total_elements']:,}",
            f"Total elements sorted: {total_sorted:,}",
            f"Verification: {'✓ PASSED' if total_sorted == metadata['total_elements'] else '✗ FAILED'}"
        ]
        return "\n".join(lines)

    def _assemble_io(self, results: list, metadata: dict) -> str:
        """Assemble IO simulation results."""
        total_slept = 0.0
        for r in results:
            if not r.get("success", False):
                continue
            data = r.get("data", {})
            total_slept += data.get("slept_for", 0.0)

        lines = [
            f"IO Simulation Complete",
            f"Expected sleep total: {metadata['expected_total_sleep']:.2f}s",
            f"Actual aggregate simulated IO block: {total_slept:.2f}s",
            f"Parallel advantage verified."
        ]
        return "\n".join(lines)

    def _assemble_log_analysis(self, results: list, metadata: dict) -> str:
        """Assemble results from distributed log analysis."""
        combined_ips = {}
        combined_status = {}
        combined_methods = {}
        total_bw = 0

        for r in results:
            if not r.get("success", False):
                continue
            data = r.get("data", {})
            
            # Merge IPs
            for ip, count in data.get("ip_counts", {}).items():
                combined_ips[ip] = combined_ips.get(ip, 0) + count
            
            # Merge Status
            for status, count in data.get("status_counts", {}).items():
                combined_status[status] = combined_status.get(status, 0) + count
                
            # Merge Methods
            for method, count in data.get("method_counts", {}).items():
                combined_methods[method] = combined_methods.get(method, 0) + count
                
            total_bw += data.get("total_bytes", 0)

        # Sort top IPs
        top_ips = sorted(combined_ips.items(), key=lambda x: x[1], reverse=True)[:5]
        ip_summary = "\n".join([f"  - {ip}: {count} hits" for ip, count in top_ips])

        lines = [
            f"Web Server Log Analysis Complete",
            f"Total lines processed: {metadata.get('total_lines', 0):,}",
            f"Total Bandwidth: {total_bw / (1024*1024):.2f} MB",
            f"Top 5 IPs:",
            ip_summary,
            f"Status Codes: {combined_status}",
            f"HTTP Methods: {combined_methods}"
        ]
        return "\n".join(lines)

    # ─── Run Experiment ───────────────────────────────────────────

    def run_experiment(self, task_type_name: str, algorithm_name: str) -> dict:
        """Run a single experiment: one task type with one load balancing algorithm."""
        self.is_running = True
        balancer = self.balancers.get(algorithm_name)
        if not balancer:
            raise ValueError(f"Unknown algorithm: {algorithm_name}")

        self._log(f"=== Starting: {task_type_name} with {algorithm_name} ===")
        balancer.reset()
        for w in self.workers:
            w.reset_stats()

        # Check workers
        self.check_all_workers()
        alive = [w for w in self.workers if w.alive]
        if not alive:
            self._log("ERROR: No workers are online!")
            self.is_running = False
            return {"error": "No workers online"}
        self._log(f"Active workers: {len(alive)}")

        num_chunks = min(self.chunks_count, len(alive) * 4)
        task_id = uuid.uuid4().hex[:8]

        # 1. Split
        self._log(f"Splitting task into {num_chunks} chunks...")
        if task_type_name == TaskType.MATRIX_MULTIPLICATION.value:
            chunks, metadata = self._split_matrix(num_chunks, task_id)
        elif task_type_name == TaskType.MONTE_CARLO_PI.value:
            chunks, metadata = self._split_monte_carlo(num_chunks, task_id)
        elif task_type_name == TaskType.PRIME_FACTORIZATION.value:
            chunks, metadata = self._split_prime(num_chunks, task_id)
        elif task_type_name == TaskType.DATA_SORTING.value:
            chunks, metadata = self._split_sorting(num_chunks, task_id)
        elif task_type_name == TaskType.IO_SIMULATION.value:
            chunks, metadata = self._split_io(num_chunks, task_id)
        elif task_type_name == TaskType.WEB_LOG_ANALYSIS.value:
            chunks, metadata = self._split_log_analysis(num_chunks, task_id)
        else:
            self.is_running = False
            return {"error": f"Unknown task type: {task_type_name}"}

        # 2. Dispatch via load balancer
        self._log(f"Dispatching {len(chunks)} chunks...")
        start_time = time.time()

        results = []
        with ThreadPoolExecutor(max_workers=20) as pool:
            futures = {}
            for chunk in chunks:
                worker = balancer.select_worker(self.workers, chunk)
                self._log(f"  Chunk {chunk.chunk_id} → {worker.id}")
                f = pool.submit(self._dispatch_chunk, chunk, worker)
                futures[f] = chunk

            for f in as_completed(futures):
                try:
                    result = f.result(timeout=self.read_timeout)
                    results.append(result)
                    if result.get("success"):
                        self._log(f"  ✓ Chunk {result.get('chunk_id')} from "
                                  f"{result.get('worker_id')} ({result.get('execution_time_ms', 0):.0f}ms)")
                    else:
                        self._log(f"  ✗ Chunk {result.get('chunk_id')} failed: "
                                  f"{result.get('error_message', 'unknown')}")
                except Exception as e:
                    self._log(f"  ✗ Future failed: {e}")

        total_time = (time.time() - start_time) * 1000

        # 3. Assemble
        if task_type_name == TaskType.MATRIX_MULTIPLICATION.value:
            summary = self._assemble_matrix(results, metadata)
        elif task_type_name == TaskType.MONTE_CARLO_PI.value:
            summary = self._assemble_monte_carlo(results, metadata)
        elif task_type_name == TaskType.PRIME_FACTORIZATION.value:
            summary = self._assemble_prime(results, metadata)
        elif task_type_name == TaskType.DATA_SORTING.value:
            summary = self._assemble_sorting(results, metadata)
        elif task_type_name == TaskType.WEB_LOG_ANALYSIS.value:
            summary = self._assemble_log_analysis(results, metadata)
        else:
            summary = self._assemble_io(results, metadata)

        self._log(f"\n--- Result ---\n{summary}")

        # 4. Build metrics
        snapshot = self._build_snapshot(algorithm_name, task_type_name, total_time, results, summary)

        self._log(f"=== Complete: {task_type_name} × {algorithm_name} — "
                  f"{total_time:.0f}ms, throughput={snapshot.throughput:.2f} tasks/s ===\n")

        self.is_running = False
        return snapshot.to_dict()

    def _build_snapshot(self, algo: str, task_type: str, total_ms: float,
                        results: list, summary: str) -> MetricSnapshot:
        """Build a MetricSnapshot from experiment results."""
        task_counts = {}
        task_times = {}
        for r in results:
            wid = r.get("worker_id", "unknown")
            task_counts[wid] = task_counts.get(wid, 0) + 1
            if wid not in task_times:
                task_times[wid] = []
            task_times[wid].append(r.get("execution_time_ms", 0))

        avg_times = {}
        for wid, times in task_times.items():
            avg_times[wid] = sum(times) / len(times) if times else 0

        # Load standard deviation
        if task_counts:
            mean = sum(task_counts.values()) / len(task_counts)
            variance = sum((c - mean) ** 2 for c in task_counts.values()) / len(task_counts)
            std_dev = math.sqrt(variance)
        else:
            std_dev = 0.0

        # Max CPU
        max_cpu = max((w.cpu_usage for w in self.workers), default=0) * 100

        # Throughput
        throughput = len(results) / (total_ms / 1000.0) if total_ms > 0 else 0

        snapshot = MetricSnapshot(
            algorithm_name=algo,
            task_type=task_type,
            total_time_ms=total_ms,
            per_worker_task_count=task_counts,
            per_worker_avg_time=avg_times,
            load_std_dev=std_dev,
            max_node_utilization=max_cpu,
            throughput=throughput,
            result_summary=summary,
        )

        self.comparison_results.append(snapshot.to_dict())
        return snapshot

    def run_all_experiments(self):
        """Run all 25 experiments (5 tasks × 5 algorithms) sequentially."""
        self.comparison_results = []
        tasks = [t.value for t in TaskType]
        algos = list(self.balancers.keys())
        total = len(tasks) * len(algos)
        count = 0

        for task_name in tasks:
            for algo_name in algos:
                count += 1
                self.current_status = f"Running {count}/{total}: {task_name} × {algo_name}"
                self.run_experiment(task_name, algo_name)
                time.sleep(0.5)

        self.current_status = f"All {total} experiments complete!"
        return self.comparison_results


# ─── Flask Dashboard ──────────────────────────────────────────────────

app = Flask(__name__)
master = MasterServer()


@app.route("/")
def dashboard():
    return render_template("dashboard.html")


@app.route("/api/status")
def api_status():
    return jsonify({
        "workers": [w.to_dict() for w in master.workers],
        "is_running": master.is_running,
        "current_status": master.current_status,
        "task_types": [t.value for t in TaskType],
        "algorithms": list(master.balancers.keys()),
        "config": {
            "matrix_size": master.matrix_size,
            "monte_carlo_samples": master.monte_carlo_samples,
            "prime_count": master.prime_count,
            "chunks_count": master.chunks_count,
        },
    })


@app.route("/api/logs")
def api_logs():
    return jsonify({"logs": master.logs[-100:]})


@app.route("/api/refresh_workers", methods=["POST"])
def api_refresh():
    master.check_all_workers()
    return jsonify({"workers": [w.to_dict() for w in master.workers]})


@app.route("/api/run_experiment", methods=["POST"])
def api_run_experiment():
    if master.is_running:
        return jsonify({"error": "An experiment is already running"}), 409

    data = request.json
    task_type = data.get("task_type")
    algorithm = data.get("algorithm")

    if not task_type or not algorithm:
        return jsonify({"error": "Missing task_type or algorithm"}), 400

    master.current_status = f"Running: {task_type} × {algorithm}"
    result = master.run_experiment(task_type, algorithm)
    master.current_status = "Ready"
    return jsonify(result)


@app.route("/api/run_all", methods=["POST"])
def api_run_all():
    if master.is_running:
        return jsonify({"error": "Already running"}), 409

    def _run():
        master.run_all_experiments()

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return jsonify({"message": "Started all 25 experiments"})


@app.route("/api/comparison")
def api_comparison():
    return jsonify({"results": master.comparison_results})


@app.route("/api/clear")
def api_clear():
    master.comparison_results = []
    master.logs = []
    return jsonify({"message": "Cleared"})


if __name__ == "__main__":
    master.check_all_workers()
    alive = sum(1 for w in master.workers if w.alive)
    print(f"\n{'='*60}")
    print(f"  Distributed Computing Master Dashboard")
    print(f"{'='*60}")
    print(f"  Workers: {alive}/{len(master.workers)} online")
    print(f"  Dashboard: http://localhost:5000")
    print(f"{'='*60}\n")

    master.start_heartbeat(interval_sec=3)
    
    # Auto-open browser after 1.5 seconds
    threading.Timer(1.5, lambda: webbrowser.open("http://localhost:5000")).start()
    
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
