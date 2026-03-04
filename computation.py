"""
Computation Engines for the 5 scientific computing tasks.
Each engine processes a TaskChunk and returns computed data.

Algorithms:
  1. Matrix Multiplication — Row-slab parallel multiply
  2. Monte Carlo Pi Estimation — Distributed random sampling
  3. Prime Factorization — Trial division on large numbers
  4. Data Sorting — Sorting large arrays
  5. IO Simulation — Simulating network/disk I/O wait
"""

import random
import math
import time


def execute_task(task_type: str, data: dict, worker_id: str) -> dict:
    """Route a task to the correct computation engine and return results."""
    start = time.time()

    if task_type == "Matrix Multiplication":
        result_data = _compute_matrix(data)
    elif task_type == "Monte Carlo Pi Estimation":
        result_data = _compute_monte_carlo(data)
    elif task_type == "Prime Factorization":
        result_data = _compute_prime_factorization(data)
    elif task_type == "Data Sorting":
        result_data = _compute_sorting(data)
    elif task_type == "IO Simulation":
        result_data = _compute_io_sim(data)
    else:
        raise ValueError(f"Unknown task type: {task_type}")

    elapsed_ms = (time.time() - start) * 1000
    return {
        "result": result_data,
        "execution_time_ms": elapsed_ms,
    }


# ─── Matrix Multiplication ────────────────────────────────────────────

def _compute_matrix(data: dict) -> dict:
    """
    Multiply a slab of rows from matrix A with full matrix B.
    Input:  rows_a (2D list, subset of rows), mat_b (full 2D list), start_row, end_row, n
    Output: result_rows (2D list), start_row, end_row, n
    """
    rows_a = data["rows_a"]
    mat_b = data["mat_b"]
    start_row = data["start_row"]
    end_row = data["end_row"]
    n = data["n"]

    num_rows = end_row - start_row
    result_rows = []

    for i in range(num_rows):
        row = []
        for j in range(n):
            s = 0.0
            for k in range(n):
                s += rows_a[i][k] * mat_b[k][j]
            row.append(round(s, 6))
        result_rows.append(row)

    return {
        "result_rows": result_rows,
        "start_row": start_row,
        "end_row": end_row,
        "n": n,
    }


# ─── Monte Carlo Pi Estimation ────────────────────────────────────────

def _compute_monte_carlo(data: dict) -> dict:
    """
    Estimate Pi using Monte Carlo random sampling.
    Input:  num_samples (int), seed (int)
    Output: num_samples (int), inside_count (int)
    """
    num_samples = data["num_samples"]
    seed = data.get("seed", None)

    rng = random.Random(seed)
    inside = 0

    for _ in range(num_samples):
        x = rng.random()
        y = rng.random()
        if x * x + y * y <= 1.0:
            inside += 1

    return {
        "num_samples": num_samples,
        "inside_count": inside,
    }


# ─── Prime Factorization ──────────────────────────────────────────────

def _compute_prime_factorization(data: dict) -> dict:
    """
    Factorize a batch of numbers using trial division.
    Input:  numbers (list of ints)
    Output: results (list of {number, factors})
    """
    numbers = data["numbers"]
    results = []

    for num in numbers:
        factors = _trial_division(abs(num))
        results.append({
            "number": num,
            "factors": factors,
        })

    return {"results": results}


def _trial_division(n: int) -> list:
    """Factorize n into prime factors using trial division."""
    if n < 2:
        return [n]
    factors = []
    d = 2
    while d * d <= n:
        while n % d == 0:
            factors.append(d)
            n //= d
        d += 1
    if n > 1:
        factors.append(n)
    return factors


# ─── Data Sorting ─────────────────────────────────────────────────────

def _compute_sorting(data: dict) -> dict:
    """
    Sort a large list of numbers.
    Input:  array (list of floats)
    Output: sorted array (list of floats)
    """
    arr = data["array"]
    sorted_arr = sorted(arr)
    return {
        "count": len(sorted_arr),
        "first_element": sorted_arr[0] if sorted_arr else None,
        "last_element": sorted_arr[-1] if sorted_arr else None,
    }


# ─── IO Simulation ────────────────────────────────────────────────────

def _compute_io_sim(data: dict) -> dict:
    """
    Simulate I/O bound tasks by sleeping.
    Input:  sleep_time (float)
    Output: slept_for (float)
    """
    sleep_time = data["sleep_time"]
    time.sleep(sleep_time)
    return {
        "slept_for": sleep_time
    }
