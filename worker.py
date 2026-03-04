"""
Worker Node — Standalone TCP Server for Distributed Computing.

Run on each machine participating in the distributed system.
Listens for task chunks from the Master, computes results, and sends them back.

Usage:
    python worker.py [port] [weight] [threads]
    python worker.py 8001 1 4

Multi-Machine Deployment:
    Machine A: python worker.py 8001 2
    Machine B: python worker.py 8001 1
    Master:    Update config.ini with each machine's IP
"""

import socket
import threading
import sys
import time
import os
import psutil

from protocol import send_message, receive_message, HEARTBEAT_REQUEST
from computation import execute_task


class WorkerServer:
    """Worker node that listens for task chunks from the Master."""

    def __init__(self, port: int, weight: int = 1, max_threads: int = 4, host_override: str = None):
        self.port = port
        self.weight = weight
        self.max_threads = max_threads
        self.active_tasks = 0
        self.active_lock = threading.Lock()
        self.running = True
        self.thread_pool = threading.Semaphore(max_threads)

        # Resolve worker ID — use actual IP for multi-machine identification
        if host_override:
            self.host_ip = host_override
        else:
            self.host_ip = self._get_local_ip()
        self.worker_id = f"{self.host_ip}:{port}"

    def _get_local_ip(self) -> str:
        """Get the actual LAN IP address of this machine."""
        try:
            # Connect to an external address to determine the outgoing interface
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "localhost"

    def start(self):
        """Start listening for incoming connections from the Master."""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", self.port))   # Accept connections from ANY interface
        server.listen(50)

        print(f"{'='*60}")
        print(f"  Distributed Worker Node")
        print(f"{'='*60}")
        print(f"  Worker ID : {self.worker_id}")
        print(f"  Listening : 0.0.0.0:{self.port}")
        print(f"  Weight    : {self.weight}")
        print(f"  Threads   : {self.max_threads}")
        print(f"  LAN IP    : {self.host_ip}")
        print(f"{'='*60}")
        print(f"  Ready to accept tasks from any Master node.")
        print()

        try:
            while self.running:
                client_sock, addr = server.accept()
                t = threading.Thread(target=self._handle_connection,
                                     args=(client_sock, addr), daemon=True)
                t.start()
        except KeyboardInterrupt:
            print(f"\n[Worker {self.worker_id}] Shutting down...")
        finally:
            server.close()

    def _handle_connection(self, sock: socket.socket, addr):
        """Handle an incoming connection (heartbeat or task)."""
        try:
            msg = receive_message(sock)

            if msg.get("type") == HEARTBEAT_REQUEST:
                self._handle_heartbeat(sock)
            elif msg.get("type") == "task":
                self._handle_task(msg, sock)
            else:
                print(f"[Worker] Unknown message type: {msg.get('type')}")

        except Exception as e:
            print(f"[Worker {self.worker_id}] Connection error from {addr}: {e}")
        finally:
            try:
                sock.close()
            except:
                pass

    def _handle_heartbeat(self, sock: socket.socket):
        """Respond to a heartbeat check from the Master."""
        try:
            process = psutil.Process(os.getpid())
            cpu = process.cpu_percent(interval=0.1) / 100.0
            mem_info = process.memory_info()
        except Exception:
            cpu = 0.0
            mem_info = None

        response = {
            "type": "heartbeat_response",
            "worker_id": self.worker_id,
            "active_tasks": self.active_tasks,
            "max_threads": self.max_threads,
            "cpu_usage": cpu,
            "free_memory": psutil.virtual_memory().available if psutil else 0,
            "total_memory": psutil.virtual_memory().total if psutil else 0,
            "alive": True,
        }
        send_message(sock, response)

    def _handle_task(self, msg: dict, sock: socket.socket):
        """Execute a computation task and send the result back."""
        self.thread_pool.acquire()
        with self.active_lock:
            self.active_tasks += 1

        chunk_id = msg.get("chunk_id", "?")
        task_type = msg.get("task_type", "unknown")
        print(f"[Worker {self.worker_id}] Received: chunk {chunk_id} ({task_type})")

        try:
            result = execute_task(task_type, msg.get("data", {}), self.worker_id)

            response = {
                "type": "task_result",
                "task_id": msg.get("task_id"),
                "chunk_id": chunk_id,
                "worker_id": self.worker_id,
                "success": True,
                "data": result["result"],
                "execution_time_ms": result["execution_time_ms"],
            }
            send_message(sock, response)
            print(f"[Worker {self.worker_id}] Completed chunk {chunk_id} in {result['execution_time_ms']:.0f}ms")

        except Exception as e:
            response = {
                "type": "task_result",
                "task_id": msg.get("task_id"),
                "chunk_id": chunk_id,
                "worker_id": self.worker_id,
                "success": False,
                "error_message": str(e),
            }
            send_message(sock, response)
            print(f"[Worker {self.worker_id}] Error on chunk {chunk_id}: {e}")

        finally:
            with self.active_lock:
                self.active_tasks -= 1
            self.thread_pool.release()


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8001
    weight = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    threads = int(sys.argv[3]) if len(sys.argv) > 3 else os.cpu_count() or 4
    host = sys.argv[4] if len(sys.argv) > 4 else None

    worker = WorkerServer(port, weight, threads, host)
    worker.start()


if __name__ == "__main__":
    main()
