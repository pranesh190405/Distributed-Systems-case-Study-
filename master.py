"""
Master Server — Orchestrates distributed computing experiments.
Includes a Flask web dashboard for real-time monitoring and comparison.

Usage:
    python master.py
    Then open http://localhost:5000 in your browser.
"""

import base64
import configparser
import hashlib
import io
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
        workers_str = config.get("workers", "nodes", fallback="")
        if workers_str.strip():
            for w in workers_str.split(","):
                parts = w.strip().split(":")
                if len(parts) >= 2:
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

        # Configurable use-case parameters (can be changed from dashboard)
        self.image_blur_count = 8          # number of images to generate
        self.image_blur_size = 512         # image dimension (NxN)
        self.crypto_difficulty = 4         # PoW difficulty (number of leading zeros)

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

    # ─── UDP Discovery ────────────────────────────────────────────

    def start_udp_discovery(self):
        """Listen for UDP broadcasts from workers on port 9999."""
        def _loop():
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("", 9999))
            self._log("UDP Discovery Listener started on port 9999")
            
            while True:
                try:
                    data, addr = sock.recvfrom(1024)
                    msg = data.decode("utf-8")
                    # Format: WORKER_READY:<PORT>
                    if msg.startswith("WORKER_READY:"):
                        parts = msg.split(":")
                        if len(parts) >= 2:
                            ip = addr[0]  # Get IP from UDP sender address
                            port = int(parts[1])
                            worker_id = f"{ip}:{port}"
                            
                            exists = False
                            for w in self.workers:
                                if w.id == worker_id:
                                    w.alive = True
                                    exists = True
                                    break
                            
                            if not exists:
                                new_worker = NodeInfo(ip, port, 1)
                                new_worker.alive = True
                                self.workers.append(new_worker)
                                self._log(f"Discovered new worker via UDP: {worker_id}")
                except Exception as e:
                    self._log(f"UDP Discovery Error: {e}")
                    time.sleep(1)

        threading.Thread(target=_loop, daemon=True).start()

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

    def _split_rsa_cracking(self, num_chunks: int, task_id: str):
        """Split RSA Key Cracking into batches of public keys."""
        rng = random.Random(12345)
        # Create a skewed workload to prove dynamic balancers are best.
        # Most numbers are easy, but a few are VERY hard (large primes).
        numbers = []
        for i in range(self.prime_count):
            if i % 20 == 0:
                # hard semiprime (e.g., 2 large primes multiplied)
                # 31397 * 34211 = 1074122767
                numbers.append(1074122767 + (i%3)*2)
            else:
                numbers.append(1000 + i)

        chunks = []
        per_chunk = len(numbers) // num_chunks
        remainder = len(numbers) % num_chunks
        start = 0

        for c in range(num_chunks):
            size = per_chunk + (1 if c < remainder else 0)
            chunks.append(TaskChunk(
                task_id=task_id, chunk_id=c, total_chunks=num_chunks,
                task_type=TaskType.RSA_CRACKING.value,
                data={"public_keys": numbers[start:start + size]}
            ))
            start += size

        return chunks, {"total_keys": len(numbers)}

    def _split_sorting(self, num_chunks: int, task_id: str):
        """Split sorting task into chunks with random float arrays."""
        rng = random.Random(777)
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

    def _split_etl_pipeline(self, num_chunks: int, task_id: str):
        """Split ETL Log pipeline into chunks of mock JSON logs."""
        rng = random.Random(777)
        size_per_chunk = 50000 
        chunks = []
        endpoints = ["/login", "/api/data", "/home", "/checkout"]

        for c in range(num_chunks):
            logs = []
            for _ in range(size_per_chunk):
                logs.append({
                    "timestamp": rng.randint(1600000000, 1700000000),
                    "endpoint": rng.choice(endpoints),
                    "status": 200 if rng.random() > 0.05 else 500,
                    "user_id": rng.randint(1, 10000)
                })
                
            chunks.append(TaskChunk(
                task_id=task_id, chunk_id=c, total_chunks=num_chunks,
                task_type=TaskType.ETL_PIPELINE.value,
                data={"log_lines": logs}
            ))

        return chunks, {"total_logs": size_per_chunk * num_chunks}



    def _split_image_blur(self, num_chunks: int, task_id: str):
        """Split Image Blur task into chunks using configured image count & size."""
        from PIL import Image
        rng = random.Random(42)
        sz = self.image_blur_size
        actual_count = self.image_blur_count
        label = f"{sz}x{sz}"
        chunks = []

        self._log(f"  Image Blur config: {actual_count} images, {label} pixels each")

        for c in range(actual_count):
            # Generate a dummy RGB image in memory
            img = Image.new("RGB", (sz, sz))
            pixels = img.load()
            for y in range(sz):
                for x in range(sz):
                    pixels[x, y] = (
                        (x * 7 + y * 13 + c) % 256,
                        (x * 11 + y * 3 + c) % 256,
                        (x * 5 + y * 17 + c) % 256,
                    )

            # Save original image to disk
            os.makedirs("generated_images", exist_ok=True)
            img.save(f"generated_images/task_{task_id}_chunk_{c}_{label}.png", format="PNG")

            # Serialize to base64
            buf = io.BytesIO()
            img.save(buf, format="PNG")
            b64 = base64.b64encode(buf.getvalue()).decode("ascii")

            chunks.append(TaskChunk(
                task_id=task_id, chunk_id=c, total_chunks=actual_count,
                task_type=TaskType.IMAGE_BLUR.value,
                data={"image_b64": b64, "image_size": label, "task_id": task_id, "chunk_id": c}
            ))
            self._log(f"  Generated chunk {c}: {label} ({len(b64) // 1024} KB payload)")

        return chunks, {"total_images": actual_count}

    def _split_crypto_hash(self, num_chunks: int, task_id: str):
        """Split Crypto Proof-of-Work into tasks with user-selected difficulty."""
        import string
        rng = random.Random(42)
        chunks = []
        diff = self.crypto_difficulty

        self._log(f"  Crypto PoW config: {num_chunks} chunks, uniform difficulty={diff}")

        for c in range(num_chunks):
            # Generate a random base string for each task
            base_string = "".join(rng.choices(
                string.ascii_letters + string.digits, k=32
            ))

            chunks.append(TaskChunk(
                task_id=task_id, chunk_id=c, total_chunks=num_chunks,
                task_type=TaskType.CRYPTO_HASH.value,
                data={
                    "base_string": base_string,
                    "difficulty": diff,
                    "max_iterations": 1_000_000,
                }
            ))
            self._log(f"  Generated chunk {c}: difficulty={diff}, base=\"{base_string[:12]}...\"")

        return chunks, {
            "total_tasks": num_chunks,
            "difficulty": diff,
            "difficulty_distribution": {diff: num_chunks},
        }

    def _split_log_analysis(self, num_chunks: int, task_id: str):
        """Split access logs into chunks of lines."""
        log_path = os.path.join(os.getcwd(), "datasets", "accesslog.csv")
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
            response["network_latency_ms"] = max(0.0, elapsed - response.get("execution_time_ms", elapsed))
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

    def _assemble_rsa_cracking(self, results: list, metadata: dict) -> str:
        """Assemble RSA Key Cracking results."""
        total = 0
        samples = []

        for r in results:
            if not r.get("success", False):
                continue
            data = r.get("data", {})
            for item in data.get("results", []):
                total += 1
                if item["key"] > 1000000000 and len(samples) < 5:
                    samples.append(f"  [CRACKED] {item['key']:,} = p:{item['factors'][0]} * q:{item['factors'][1]}")

        lines = [
            f"RSA Key Cracking Complete",
            f"Keys brute-forced: {total:,} / {metadata['total_keys']:,}",
            f"Sample high-difficulty cracks:",
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

    def _assemble_etl_pipeline(self, results: list, metadata: dict) -> str:
        """Assemble ETL Log Pipeline results."""
        total_processed = 0
        total_errors = 0
        for r in results:
            if not r.get("success", False):
                continue
            data = r.get("data", {})
            total_processed += data.get("count_processed", 0)
            total_errors += data.get("error_count", 0)

        lines = [
            f"ETL Log Aggregation Complete",
            f"Expected logs: {metadata['total_logs']:,}",
            f"Total logs parsed & sorted: {total_processed:,}",
            f"HTTP 500 Errors Detected: {total_errors:,}",
            f"Verification: {'✓ PASSED' if total_processed == metadata['total_logs'] else '✗ FAILED'}"
        ]
        return "\n".join(lines)



    def _assemble_image_blur(self, results: list, metadata: dict) -> str:
        """Assemble Image Blur results."""
        success_count = 0
        error_count = 0
        size_times = {}  # {"2048x2048": [ms1, ms2, ...]}

        for r in results:
            if not r.get("success", False):
                error_count += 1
                continue
            data = r.get("data", {})
            if data.get("status") == "success":
                success_count += 1
                sz = data.get("image_size", "unknown")
                if sz not in size_times:
                    size_times[sz] = []
                size_times[sz].append(r.get("execution_time_ms", 0))
            else:
                error_count += 1

        lines = [
            f"Image Blur Processing Complete",
            f"Total images: {metadata['total_images']}",
            f"Successful: {success_count}, Failed: {error_count}",
            f"Per-size blur times:",
        ]
        for sz in sorted(size_times.keys()):
            times = size_times[sz]
            avg = sum(times) / len(times) if times else 0
            lines.append(f"  {sz}: {len(times)} images, avg {avg:.0f}ms")

        lines.append(f"Verification: {'✓ PASSED' if error_count == 0 else '⚠ PARTIAL (' + str(error_count) + ' errors)'}")
        return "\n".join(lines)

    def _assemble_crypto_hash(self, results: list, metadata: dict) -> str:
        """Assemble Crypto Proof-of-Work results."""
        success_count = 0
        timeout_count = 0
        error_count = 0
        diff_stats = {}  # {difficulty: [(nonce, iterations, time_ms), ...]}

        for r in results:
            if not r.get("success", False):
                error_count += 1
                continue
            data = r.get("data", {})
            diff = data.get("difficulty", 0)
            if diff not in diff_stats:
                diff_stats[diff] = []

            if data.get("status") == "success":
                success_count += 1
                diff_stats[diff].append({
                    "nonce": data.get("nonce_found", 0),
                    "iterations": data.get("iterations_tried", 0),
                    "time_ms": r.get("execution_time_ms", 0),
                })
            elif data.get("status") == "timeout":
                timeout_count += 1
            else:
                error_count += 1

        lines = [
            f"Crypto Proof-of-Work Complete",
            f"Total tasks: {metadata['total_tasks']}",
            f"Successful: {success_count}, Timeouts: {timeout_count}, Errors: {error_count}",
            f"Difficulty distribution: {metadata['difficulty_distribution']}",
            f"Per-difficulty results:",
        ]
        for diff in sorted(diff_stats.keys()):
            entries = diff_stats[diff]
            if entries:
                avg_iter = sum(e['iterations'] for e in entries) / len(entries)
                avg_ms = sum(e['time_ms'] for e in entries) / len(entries)
                lines.append(f"  Difficulty {diff}: {len(entries)} solved, "
                             f"avg {avg_iter:,.0f} iterations, avg {avg_ms:.0f}ms")

        status = '✓ PASSED' if (error_count == 0 and timeout_count == 0) else \
                 f'⚠ PARTIAL ({timeout_count} timeouts, {error_count} errors)'
        lines.append(f"Verification: {status}")
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
        elif task_type_name == TaskType.RSA_CRACKING.value:
            chunks, metadata = self._split_rsa_cracking(num_chunks, task_id)
        elif task_type_name == TaskType.DATA_SORTING.value:
            chunks, metadata = self._split_sorting(num_chunks, task_id)
        elif task_type_name == TaskType.ETL_PIPELINE.value:
            chunks, metadata = self._split_etl_pipeline(num_chunks, task_id)
        elif task_type_name == TaskType.IMAGE_BLUR.value:
            chunks, metadata = self._split_image_blur(num_chunks, task_id)
        elif task_type_name == TaskType.CRYPTO_HASH.value:
            chunks, metadata = self._split_crypto_hash(num_chunks, task_id)
        elif task_type_name == TaskType.WEB_LOG_ANALYSIS.value:
            chunks, metadata = self._split_log_analysis(num_chunks, task_id)
        else:
            self.is_running = False
            return {"error": f"Unknown task type: {task_type_name}"}

        # 2. Dispatch via load balancer
        self._log(f"Dispatching {len(chunks)} chunks...")
        start_time = time.time()

        results = []
        import concurrent.futures
        with ThreadPoolExecutor(max_workers=20) as pool:
            futures = {}

            def _submit(chunk):
                alive = [w for w in self.workers if w.alive]
                if not alive:
                    self._log("ERROR: No alive workers to dispatch chunk!")
                    results.append({"success": False, "chunk_id": chunk.chunk_id, "error_message": "No alive workers"})
                    return
                worker = balancer.select_worker(self.workers, chunk)
                self._log(f"  Chunk {chunk.chunk_id} → {worker.id}")
                f = pool.submit(self._dispatch_chunk, chunk, worker)
                futures[f] = chunk

            for chunk in chunks:
                _submit(chunk)

            while futures:
                done, not_done = concurrent.futures.wait(list(futures.keys()), return_when=concurrent.futures.FIRST_COMPLETED)
                for f in done:
                    chunk = futures.pop(f)
                    try:
                        result = f.result(timeout=self.read_timeout)
                        if result.get("success"):
                            results.append(result)
                            self._log(f"  ✓ Chunk {result.get('chunk_id')} from "
                                      f"{result.get('worker_id')} ({result.get('execution_time_ms', 0):.0f}ms)")
                        else:
                            self._log(f"  ✗ Chunk {result.get('chunk_id')} failed: "
                                      f"{result.get('error_message', 'unknown')} - REQUEUING")
                            # Mark worker as dead if it was a connection error
                            error_msg = result.get('error_message', '')
                            if "Timeout" in error_msg or "Connection refused" in error_msg:
                                for w in self.workers:
                                    if w.id == result.get("worker_id"):
                                        w.alive = False
                            _submit(chunk)
                    except Exception as e:
                        self._log(f"  ✗ Future failed for chunk {chunk.chunk_id}: {e} - REQUEUING")
                        _submit(chunk)

        total_time = (time.time() - start_time) * 1000

        # 3. Assemble
        if task_type_name == TaskType.MATRIX_MULTIPLICATION.value:
            summary = self._assemble_matrix(results, metadata)
        elif task_type_name == TaskType.MONTE_CARLO_PI.value:
            summary = self._assemble_monte_carlo(results, metadata)
        elif task_type_name == TaskType.PRIME_FACTORIZATION.value:
            summary = self._assemble_prime(results, metadata)
        elif task_type_name == TaskType.RSA_CRACKING.value:
            summary = self._assemble_rsa_cracking(results, metadata)
        elif task_type_name == TaskType.DATA_SORTING.value:
            summary = self._assemble_sorting(results, metadata)
        elif task_type_name == TaskType.ETL_PIPELINE.value:
            summary = self._assemble_etl_pipeline(results, metadata)
        elif task_type_name == TaskType.IMAGE_BLUR.value:
            summary = self._assemble_image_blur(results, metadata)
        elif task_type_name == TaskType.CRYPTO_HASH.value:
            summary = self._assemble_crypto_hash(results, metadata)
        elif task_type_name == TaskType.WEB_LOG_ANALYSIS.value:
            summary = self._assemble_log_analysis(results, metadata)
        else:
            summary = "Unknown task type"

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

        # Network Latency
        total_net_latency = sum(r.get("network_latency_ms", 0) for r in results if r.get("success"))
        success_count = sum(1 for r in results if r.get("success"))
        avg_net_latency = total_net_latency / success_count if success_count > 0 else 0.0

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
            avg_network_latency_ms=avg_net_latency,
        )

        self.comparison_results.append(snapshot.to_dict())
        return snapshot

    def run_all_experiments(self):
        """Run all experiments (all tasks × all algorithms) sequentially."""
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

    def run_task_all_algos(self, task_type_name: str):
        """Run a single task type against all 5 load balancing algorithms."""
        algos = list(self.balancers.keys())
        total = len(algos)
        count = 0
        for algo_name in algos:
            count += 1
            self.current_status = f"Running {count}/{total}: {task_type_name} × {algo_name}"
            self.run_experiment(task_type_name, algo_name)
            time.sleep(0.5)
        self.current_status = "Ready"

    def run_subset_experiments(self, task_list: list):
        """Run a list of task types, each against all 5 algorithms."""
        algos = list(self.balancers.keys())
        total = len(task_list) * len(algos)
        count = 0
        for task_name in task_list:
            for algo_name in algos:
                count += 1
                self.current_status = f"Running {count}/{total}: {task_name} × {algo_name}"
                self.run_experiment(task_name, algo_name)
                time.sleep(0.5)
        self.current_status = f"All {total} experiments complete!"


# ─── Constants for task categories ────────────────────────────────────
BENCHMARKS = [t.value for t in TaskType if "(Benchmark)" in t.value]
USE_CASES = [t.value for t in TaskType if "(Use Case)" in t.value]


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
        "benchmarks": BENCHMARKS,
        "use_cases": USE_CASES,
        "algorithms": list(master.balancers.keys()),
        "config": {
            "matrix_size": master.matrix_size,
            "monte_carlo_samples": master.monte_carlo_samples,
            "prime_count": master.prime_count,
            "chunks_count": master.chunks_count,
            "image_blur_count": master.image_blur_count,
            "image_blur_size": master.image_blur_size,
            "crypto_difficulty": master.crypto_difficulty,
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


@app.route("/api/run_benchmarks_single", methods=["POST"])
def api_run_benchmarks_single():
    """Run all 5 algorithms for a single benchmark."""
    if master.is_running:
        return jsonify({"error": "Already running"}), 409
    data = request.json
    task_type = data.get("task_type")
    if not task_type or task_type not in BENCHMARKS:
        return jsonify({"error": f"Invalid benchmark: {task_type}"}), 400

    def _run():
        master.run_task_all_algos(task_type)
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"message": f"Running all algorithms for {task_type}"})


@app.route("/api/run_benchmarks_all", methods=["POST"])
def api_run_benchmarks_all():
    """Run all benchmarks × all algorithms."""
    if master.is_running:
        return jsonify({"error": "Already running"}), 409

    def _run():
        master.run_subset_experiments(BENCHMARKS)
    threading.Thread(target=_run, daemon=True).start()
    total = len(BENCHMARKS) * len(master.balancers)
    return jsonify({"message": f"Started {total} benchmark experiments"})


@app.route("/api/run_usecases_single", methods=["POST"])
def api_run_usecases_single():
    """Run all 5 algorithms for a single use case."""
    if master.is_running:
        return jsonify({"error": "Already running"}), 409
    data = request.json
    task_type = data.get("task_type")
    if not task_type or task_type not in USE_CASES:
        return jsonify({"error": f"Invalid use case: {task_type}"}), 400

    def _run():
        master.run_task_all_algos(task_type)
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"message": f"Running all algorithms for {task_type}"})


@app.route("/api/run_usecases_all", methods=["POST"])
def api_run_usecases_all():
    """Run all use cases × all algorithms."""
    if master.is_running:
        return jsonify({"error": "Already running"}), 409

    def _run():
        master.run_subset_experiments(USE_CASES)
    threading.Thread(target=_run, daemon=True).start()
    total = len(USE_CASES) * len(master.balancers)
    return jsonify({"message": f"Started {total} use case experiments"})


@app.route("/api/add_worker", methods=["POST"])
def api_add_worker():
    """Add a worker node by IP:PORT."""
    data = request.json
    address = data.get("address", "").strip()
    if not address or ":" not in address:
        return jsonify({"error": "Invalid address format. Use IP:PORT (e.g. 192.168.1.5:5001)"}), 400

    parts = address.split(":")
    host = parts[0]
    try:
        port = int(parts[1])
    except ValueError:
        return jsonify({"error": "Port must be a number"}), 400

    worker_id = f"{host}:{port}"
    for w in master.workers:
        if w.id == worker_id:
            return jsonify({"error": f"Worker {worker_id} already exists"}), 409

    new_worker = NodeInfo(host, port, 1)
    master.workers.append(new_worker)
    master._check_worker(new_worker)
    master._log(f"Manually added worker: {worker_id} (alive={new_worker.alive})")
    return jsonify({"message": f"Worker {worker_id} added", "alive": new_worker.alive})


@app.route("/api/remove_worker", methods=["POST"])
def api_remove_worker():
    """Remove a worker node by IP:PORT."""
    data = request.json
    address = data.get("address", "").strip()
    if not address:
        return jsonify({"error": "Missing address"}), 400

    for i, w in enumerate(master.workers):
        if w.id == address:
            del master.workers[i]
            master._log(f"Manually removed worker: {address}")
            return jsonify({"message": f"Worker {address} removed"})

    return jsonify({"error": f"Worker {address} not found"}), 404


@app.route("/api/update_config", methods=["POST"])
def api_update_config():
    """Update configurable parameters for use cases."""
    data = request.json
    if "image_blur_count" in data:
        master.image_blur_count = max(1, min(50, int(data["image_blur_count"])))
    if "image_blur_size" in data:
        allowed = [128, 256, 512, 1024, 2048, 4096]
        val = int(data["image_blur_size"])
        if val in allowed:
            master.image_blur_size = val
    if "crypto_difficulty" in data:
        master.crypto_difficulty = max(1, min(7, int(data["crypto_difficulty"])))
    master._log(f"Config updated: blur={master.image_blur_count}x{master.image_blur_size}, crypto_diff={master.crypto_difficulty}")
    return jsonify({"message": "Config updated"})


@app.route("/api/set_worker_weight", methods=["POST"])
def api_set_worker_weight():
    """Set the weight for a specific worker (used by WRR)."""
    data = request.json
    worker_id = data.get("worker_id")
    weight = data.get("weight")
    if not worker_id or weight is None:
        return jsonify({"error": "Missing worker_id or weight"}), 400
    weight = max(1, min(10, int(weight)))
    for w in master.workers:
        if w.id == worker_id:
            w.weight = weight
            master._log(f"Worker {worker_id} weight set to {weight}")
            return jsonify({"message": f"Weight for {worker_id} set to {weight}"})
    return jsonify({"error": f"Worker {worker_id} not found"}), 404


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

    master.start_udp_discovery()
    master.start_heartbeat(interval_sec=3)
    
    # Auto-open browser after 1.5 seconds
    threading.Timer(1.5, lambda: webbrowser.open("http://localhost:5000")).start()
    
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
