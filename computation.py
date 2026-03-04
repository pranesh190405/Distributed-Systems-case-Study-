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

import base64
import io
import random
import math
import time
import traceback


def execute_task(task_type: str, data: dict, worker_id: str) -> dict:
    """Route a task to the correct computation engine and return results."""
    start = time.time()

    if task_type == "Matrix Multiplication (Benchmark)":
        result_data = _compute_matrix(data)
    elif task_type == "ML Batch Inference (Use Case)":
        result_data = _compute_ml_inference(data)
    elif task_type == "Monte Carlo Pi (Benchmark)":
        result_data = _compute_monte_carlo(data)
    elif task_type == "Financial Option Pricing (Use Case)":
        result_data = _compute_financial_pricing(data)
    elif task_type == "Prime Factorization (Benchmark)":
        result_data = _compute_prime_factorization(data)
    elif task_type == "RSA Key Cracking (Use Case)":
        result_data = _compute_rsa_cracking(data)
    elif task_type == "Data Sorting (Benchmark)":
        result_data = _compute_sorting(data)
    elif task_type == "ETL Log Aggregation (Use Case)":
        result_data = _compute_etl_pipeline(data)
    elif task_type == "IO Simulation (Benchmark)":
        result_data = _compute_io_sim(data)
    elif task_type == "Distributed Web Crawler (Use Case)":
        result_data = _compute_web_scraper(data)
    elif task_type == "Image Blur (Use Case)":
        result_data = _compute_image_blur(data)
    else:
        raise ValueError(f"Unknown task type: {task_type}")

    elapsed_ms = (time.time() - start) * 1000
    return {
        "result": result_data,
        "execution_time_ms": elapsed_ms,
    }


# ─── Matrix Multiplication (Benchmark) ──────────────────────────────
def _compute_matrix(data: dict) -> dict:
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
            s = sum(rows_a[i][k] * mat_b[k][j] for k in range(n))
            row.append(round(s, 6))
        result_rows.append(row)
    return {"result_rows": result_rows, "start_row": start_row, "end_row": end_row, "n": n}

# ─── ML Batch Inference (Use Case) ────────────────────────────────────
def _compute_ml_inference(data: dict) -> dict:
    """
    Simulate a batch inference pass through a Neural Network layer with ReLU.
    Input:  batch_x (2D list), weights (2D list), bias (1D list), start_idx, end_idx
    Output: predictions (2D list), start_idx, end_idx
    """
    batch_x = data["batch_x"]
    weights = data["weights"]
    bias = data["bias"]
    start_idx = data["start_idx"]
    end_idx = data["end_idx"]
    n_features = len(weights)
    n_neurons = len(weights[0])

    num_samples = end_idx - start_idx
    predictions = []

    for i in range(num_samples):
        row = []
        for j in range(n_neurons):
            s = 0.0
            for k in range(n_features):
                s += batch_x[i][k] * weights[k][j]
            s += bias[j]
            # ReLU Activation
            row.append(max(0.0, round(s, 6)))
        predictions.append(row)

    return {
        "predictions": predictions,
        "start_idx": start_idx,
        "end_idx": end_idx,
    }


# ─── Monte Carlo Pi (Benchmark) ──────────────────────────────────
def _compute_monte_carlo(data: dict) -> dict:
    num_samples = data["num_samples"]
    seed = data.get("seed", None)
    rng = random.Random(seed)
    inside = sum(1 for _ in range(num_samples) if (rng.random()**2 + rng.random()**2) <= 1.0)
    return {"num_samples": num_samples, "inside_count": inside}

# ─── Financial Option Pricing (Use Case) ─────────────────────────
def _compute_financial_pricing(data: dict) -> dict:
    """
    Estimate European Call Option price using Black-Scholes Monte Carlo simulation.
    Input:  num_paths (int), S0, K, T, r, sigma, seed
    """
    num_paths = data["num_paths"]
    S0 = data["S0"]
    K = data["K"]
    T = data["T"]
    r = data["r"]
    sigma = data["sigma"]
    seed = data.get("seed", 42)

    rng = random.Random(seed)
    payoff_sum = 0.0

    # Precompute drift
    drift = (r - 0.5 * sigma * sigma) * T
    vol = sigma * math.sqrt(T)

    for _ in range(num_paths):
        # Approximate standard normal using Box-Muller or just sum of uniform
        # Let's use simple normal approximation for speed, or random.gauss if supported
        Z = rng.gauss(0, 1)
        ST = S0 * math.exp(drift + vol * Z)
        payoff = max(ST - K, 0)
        payoff_sum += payoff

    return {
        "num_paths": num_paths,
        "payoff_sum": payoff_sum,
    }


# ─── Prime Factorization (Benchmark) ─────────────────────────────
def _compute_prime_factorization(data: dict) -> dict:
    numbers = data["numbers"]
    results = [{"number": num, "factors": _crack_semiprime(abs(num))} for num in numbers]
    return {"results": results}

# ─── RSA Key Cracking (Use Case) ─────────────────────────────────
def _compute_rsa_cracking(data: dict) -> dict:
    """
    Factorize a batch of large numbers (semiprimes represent RSA keys).
    Input:  public_keys (list of ints)
    """
    keys = data["public_keys"]
    results = []

    for key in keys:
        factors = _crack_semiprime(abs(key))
        results.append({
            "key": key,
            "factors": factors,
        })

    return {"results": results}


def _crack_semiprime(n: int) -> list:
    """Trial division optimized to find two large prime factors of n."""
    if n < 2:
        return [n]
    factors = []
    d = 2
    # Heavy CPU bottleneck loop (simulating brute force)
    while d * d <= n:
        while n % d == 0:
            factors.append(d)
            n //= d
        d += 1
    if n > 1:
        factors.append(n)
    return factors


# ─── Data Sorting (Benchmark) ────────────────────────────────────
def _compute_sorting(data: dict) -> dict:
    arr = data["array"]
    sorted_arr = sorted(arr)
    return {
        "count": len(sorted_arr),
        "first_element": sorted_arr[0] if sorted_arr else None,
        "last_element": sorted_arr[-1] if sorted_arr else None,
    }

# ─── ETL Log Aggregation (Use Case) ──────────────────────────────
def _compute_etl_pipeline(data: dict) -> dict:
    """
    Parse a large batch of simulated server logs, extract status codes, and sort by timestamp.
    Input:  log_lines (list of dicts)
    """
    logs = data["log_lines"]
    
    # Simulate parsing & extraction
    parsed = []
    error_count = 0
    for log in logs:
        # Heavily memory bound processing
        if log.get("status") >= 400:
            error_count += 1
        parsed.append(log)

    # Sort operations are memory intensive for large objects
    parsed.sort(key=lambda x: x["timestamp"])

    return {
        "count_processed": len(parsed),
        "error_count": error_count,
        "earliest_ts": parsed[0]["timestamp"] if parsed else None,
        "latest_ts": parsed[-1]["timestamp"] if parsed else None,
    }


# ─── IO Simulation (Benchmark) ───────────────────────────────────
def _compute_io_sim(data: dict) -> dict:
    sleep_time = data["sleep_time"]
    time.sleep(sleep_time)
    return {"slept_for": sleep_time}

# ─── Distributed Web Crawler (Use Case) ──────────────────────────
def _compute_web_scraper(data: dict) -> dict:
    """
    Simulate fetching public APIs or large web pages where latency varies wildly due to network conditions.
    """
    target_url = data.get("target_url", "http://example.com")
    simulated_latency = data.get("simulated_latency", 0.5)
    
    # We use sleep to avoid actually hammering an external server to death in a benchmark.
    # It perfectly mimics thread-blocking IO wait.
    time.sleep(simulated_latency)
    
    return {
        "url_scraped": target_url,
        "bytes_downloaded": int(random.Random().uniform(1024, 50000)), # mock data size
        "latency_sec": simulated_latency
    }

# ─── Image Blur — CPU-Bound (Use Case) ───────────────────────────
def _compute_image_blur(data: dict) -> dict:
    """
    Apply a heavy GaussianBlur to a base64-encoded image.
    The processed image is discarded — this is purely a CPU benchmark.
    Input:  image_b64 (str), image_size (str e.g. "2048x2048")
    """
    try:
        from PIL import Image, ImageFilter

        raw_bytes = base64.b64decode(data["image_b64"])
        img = Image.open(io.BytesIO(raw_bytes))
        img.load()  # Force full decode to catch corruption early

        # Heavy CPU-bound operation
        blurred = img.filter(ImageFilter.GaussianBlur(radius=15))

        # Discard — do NOT save to disk
        del blurred

        return {
            "status": "success",
            "task_type": "image_blur",
            "image_size": data.get("image_size", "unknown"),
        }

    except Exception as e:
        return {
            "status": "error",
            "task_type": "image_blur",
            "image_size": data.get("image_size", "unknown"),
            "error": str(e),
            "traceback": traceback.format_exc(),
        }
