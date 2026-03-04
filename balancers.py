"""
Load Balancing Algorithms for distributing tasks across worker nodes.

Implements 5 algorithms:
  1. Round Robin          — Cyclic assignment
  2. Weighted Round Robin — Proportional to node weight
  3. Least Connections    — Worker with fewest active tasks
  4. Least Response Time  — Worker with lowest avg response time
  5. Random               — Random worker selection (baseline)
"""

import random
import threading


class LoadBalancer:
    """Base interface for load balancing algorithms."""

    def select_worker(self, nodes, chunk=None):
        """Select a worker node for the given task chunk."""
        raise NotImplementedError

    def reset(self):
        """Reset internal state between experiment runs."""
        pass

    @property
    def name(self):
        raise NotImplementedError


class RoundRobinBalancer(LoadBalancer):
    """
    Round Robin: Assigns tasks cyclically to workers, ignoring current state.
    
    Algorithm:
        counter = 0
        for each task:
            worker = alive_workers[counter % len(alive_workers)]
            counter += 1
    
    Time Complexity:  O(n) per selection (filtering alive nodes)
    Space Complexity: O(1) extra
    Trade-off:        Simple & fair, but ignores actual load differences
    """

    def __init__(self):
        self._counter = 0
        self._lock = threading.Lock()

    def select_worker(self, nodes, chunk=None):
        alive = [n for n in nodes if n.alive]
        if not alive:
            raise RuntimeError("No alive workers available")
        with self._lock:
            idx = self._counter % len(alive)
            self._counter += 1
        return alive[idx]

    def reset(self):
        with self._lock:
            self._counter = 0

    @property
    def name(self):
        return "Round Robin"


class WeightedRoundRobinBalancer(LoadBalancer):
    """
    Weighted Round Robin: Higher-weight workers receive proportionally more tasks.
    
    Algorithm:
        expanded_list = []
        for each alive worker:
            add worker to expanded_list 'weight' times
        worker = expanded_list[counter % len(expanded_list)]
        counter += 1
    
    Time Complexity:  O(W) per selection (W = total weight sum)
    Space Complexity: O(W) for expanded list
    Trade-off:        Accounts for heterogeneous machines, but static weights
    """

    def __init__(self):
        self._counter = 0
        self._lock = threading.Lock()

    def select_worker(self, nodes, chunk=None):
        alive = [n for n in nodes if n.alive]
        if not alive:
            raise RuntimeError("No alive workers available")

        # Build expanded list based on weights
        expanded = []
        for node in alive:
            w = max(1, node.weight)
            for _ in range(w):
                expanded.append(node)

        with self._lock:
            idx = self._counter % len(expanded)
            self._counter += 1
        return expanded[idx]

    def reset(self):
        with self._lock:
            self._counter = 0

    @property
    def name(self):
        return "Weighted Round Robin"


class LeastConnectionsBalancer(LoadBalancer):
    """
    Least Connections: Sends the task to the worker with the fewest active tasks.
    
    Algorithm:
        worker = min(alive_workers, key=active_tasks)
    
    Time Complexity:  O(n) per selection
    Space Complexity: O(1)
    Trade-off:        Adapts to real-time load, but doesn't consider task complexity
    """

    def select_worker(self, nodes, chunk=None):
        alive = [n for n in nodes if n.alive]
        if not alive:
            raise RuntimeError("No alive workers available")
        return min(alive, key=lambda n: n.active_tasks)

    @property
    def name(self):
        return "Least Connections"


class LeastResponseTimeBalancer(LoadBalancer):
    """
    Least Response Time: Sends the task to the worker with the lowest average
    response time. Falls back to Least Connections if no data available.
    
    Algorithm:
        if any worker has completed tasks:
            worker = min(alive_workers, key=avg_response_time)
        else:
            worker = min(alive_workers, key=active_tasks)  # fallback
    
    Time Complexity:  O(n) per selection
    Space Complexity: O(1)
    Trade-off:        Best adaptability, but needs warm-up period
    """

    def select_worker(self, nodes, chunk=None):
        alive = [n for n in nodes if n.alive]
        if not alive:
            raise RuntimeError("No alive workers available")

        # Fall back to least connections if no data yet
        any_has_data = any(n.completed_tasks > 0 for n in alive)
        if not any_has_data:
            return min(alive, key=lambda n: n.active_tasks)

        # Pick worker with lowest average response time
        return min(alive, key=lambda n: n.avg_response_time if n.completed_tasks > 0 else float("-inf"))

    @property
    def name(self):
        return "Least Response Time"


class RandomBalancer(LoadBalancer):
    """
    Random: Selects a random worker. Used as a baseline for comparison.
    
    Algorithm:
        worker = random.choice(alive_workers)
    
    Time Complexity:  O(n) per selection (filtering)
    Space Complexity: O(1)
    Trade-off:        No overhead, but no intelligence — purely probabilistic
    """

    def select_worker(self, nodes, chunk=None):
        alive = [n for n in nodes if n.alive]
        if not alive:
            raise RuntimeError("No alive workers available")
        return random.choice(alive)

    @property
    def name(self):
        return "Random"


# Registry of all available balancers
ALL_BALANCERS = [
    RoundRobinBalancer(),
    WeightedRoundRobinBalancer(),
    LeastConnectionsBalancer(),
    LeastResponseTimeBalancer(),
    RandomBalancer(),
]

def get_balancer_map() -> dict:
    """Return a name -> balancer mapping."""
    return {b.name: b for b in ALL_BALANCERS}
