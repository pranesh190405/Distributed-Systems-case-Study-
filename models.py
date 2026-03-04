"""
Data models for the Distributed Scientific Computing system.
Contains TaskChunk, TaskResult, NodeInfo, and MetricSnapshot.
"""

import time
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class TaskType(Enum):
    MATRIX_MULTIPLICATION = "Matrix Multiplication (Benchmark)"
    ML_INFERENCE = "ML Batch Inference (Use Case)"
    MONTE_CARLO_PI = "Monte Carlo Pi (Benchmark)"
    FINANCIAL_PRICING = "Financial Option Pricing (Use Case)"
    PRIME_FACTORIZATION = "Prime Factorization (Benchmark)"
    RSA_CRACKING = "RSA Key Cracking (Use Case)"
    DATA_SORTING = "Data Sorting (Benchmark)"
    ETL_PIPELINE = "ETL Log Aggregation (Use Case)"
    IO_SIMULATION = "IO Simulation (Benchmark)"
    WEB_CRAWLER = "Distributed Web Crawler (Use Case)"


@dataclass
class TaskChunk:
    """A chunk of work sent from Master to a Worker."""
    task_id: str
    chunk_id: int
    total_chunks: int
    task_type: str       # TaskType value string
    data: dict           # Task-specific payload
    timestamp: float = field(default_factory=time.time)

    def to_dict(self):
        return {
            "task_id": self.task_id,
            "chunk_id": self.chunk_id,
            "total_chunks": self.total_chunks,
            "task_type": self.task_type,
            "data": self.data,
            "timestamp": self.timestamp,
        }

    @staticmethod
    def from_dict(d):
        return TaskChunk(
            task_id=d["task_id"],
            chunk_id=d["chunk_id"],
            total_chunks=d["total_chunks"],
            task_type=d["task_type"],
            data=d["data"],
            timestamp=d.get("timestamp", time.time()),
        )


@dataclass
class TaskResult:
    """Result returned by a Worker after computing a TaskChunk."""
    task_id: str
    chunk_id: int
    worker_id: str
    success: bool
    data: Optional[dict] = None
    execution_time_ms: float = 0.0
    error_message: Optional[str] = None

    def to_dict(self):
        return {
            "task_id": self.task_id,
            "chunk_id": self.chunk_id,
            "worker_id": self.worker_id,
            "success": self.success,
            "data": self.data,
            "execution_time_ms": self.execution_time_ms,
            "error_message": self.error_message,
        }

    @staticmethod
    def from_dict(d):
        return TaskResult(
            task_id=d["task_id"],
            chunk_id=d["chunk_id"],
            worker_id=d["worker_id"],
            success=d["success"],
            data=d.get("data"),
            execution_time_ms=d.get("execution_time_ms", 0),
            error_message=d.get("error_message"),
        )


class NodeInfo:
    """Describes a worker node and its current state."""

    def __init__(self, host: str, port: int, weight: int = 1):
        self.host = host
        self.port = port
        self.weight = weight
        self.id = f"{host}:{port}"
        self.alive = False
        self.active_tasks = 0
        self.completed_tasks = 0
        self.total_response_time = 0.0
        self.cpu_usage = 0.0
        self.memory_usage = 0
        self.total_memory = 0

    @property
    def avg_response_time(self):
        if self.completed_tasks == 0:
            return 0.0
        return self.total_response_time / self.completed_tasks

    def record_task_completion(self, response_time_ms: float):
        self.total_response_time += response_time_ms
        self.completed_tasks += 1

    def reset_stats(self):
        self.active_tasks = 0
        self.completed_tasks = 0
        self.total_response_time = 0.0

    def to_dict(self):
        return {
            "id": self.id,
            "host": self.host,
            "port": self.port,
            "weight": self.weight,
            "alive": self.alive,
            "active_tasks": self.active_tasks,
            "completed_tasks": self.completed_tasks,
            "avg_response_time": round(self.avg_response_time, 1),
            "cpu_usage": round(self.cpu_usage * 100, 1),
            "memory_usage_mb": round(self.memory_usage / (1024 * 1024), 1) if self.memory_usage else 0,
        }


@dataclass
class MetricSnapshot:
    """Snapshot of metrics for one experiment run."""
    algorithm_name: str = ""
    task_type: str = ""
    total_time_ms: float = 0
    per_worker_task_count: dict = field(default_factory=dict)
    per_worker_avg_time: dict = field(default_factory=dict)
    load_std_dev: float = 0.0
    max_node_utilization: float = 0.0
    throughput: float = 0.0
    result_summary: str = ""

    def to_dict(self):
        return {
            "algorithm_name": self.algorithm_name,
            "task_type": self.task_type,
            "total_time_ms": round(self.total_time_ms),
            "per_worker_task_count": self.per_worker_task_count,
            "per_worker_avg_time": {k: round(v, 1) for k, v in self.per_worker_avg_time.items()},
            "load_std_dev": round(self.load_std_dev, 3),
            "max_node_utilization": round(self.max_node_utilization, 1),
            "throughput": round(self.throughput, 2),
            "result_summary": self.result_summary,
        }
