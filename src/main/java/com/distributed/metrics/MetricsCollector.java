package com.distributed.metrics;

import com.distributed.model.MetricSnapshot;
import com.distributed.model.NodeInfo;
import com.distributed.model.TaskResult;
import com.distributed.model.TaskType;
import com.distributed.network.HeartbeatMessage;
import com.distributed.network.NetworkProtocol;

import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

/**
 * Collects and aggregates metrics from worker nodes and experiment runs.
 */
public class MetricsCollector {

    private final List<NodeInfo> workers;
    private final Map<String, List<MetricSnapshot>> history = new ConcurrentHashMap<>();
    private ScheduledExecutorService heartbeatScheduler;

    public MetricsCollector(List<NodeInfo> workers) {
        this.workers = workers;
    }

    /**
     * Start periodic heartbeat monitoring of workers.
     */
    public void startHeartbeatMonitoring(int intervalMs) {
        heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            for (NodeInfo worker : workers) {
                checkWorkerHealth(worker);
            }
        }, 0, intervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Check a single worker's health via heartbeat.
     */
    private void checkWorkerHealth(NodeInfo worker) {
        try {
            Socket socket = new Socket(worker.getHost(), worker.getPort());
            socket.setSoTimeout(5000);
            NetworkProtocol.sendObject(socket, NetworkProtocol.HEARTBEAT_REQUEST);
            Object response = NetworkProtocol.receiveObject(socket);
            socket.close();

            if (response instanceof HeartbeatMessage hb) {
                worker.setAlive(true);
                worker.setCpuUsage(hb.getCpuUsage());
                worker.setMemoryUsage(hb.getTotalMemory() - hb.getFreeMemory());
            }
        } catch (Exception e) {
            worker.setAlive(false);
        }
    }

    /**
     * Perform a single health check pass on all workers (blocking).
     */
    public void checkAllWorkers() {
        for (NodeInfo worker : workers) {
            checkWorkerHealth(worker);
        }
    }

    /**
     * Build a MetricSnapshot from completed experiment results.
     */
    public MetricSnapshot buildSnapshot(String algorithmName, TaskType taskType,
                                        long totalTimeMs, List<TaskResult> results,
                                        String resultSummary) {
        MetricSnapshot snapshot = new MetricSnapshot();
        snapshot.setAlgorithmName(algorithmName);
        snapshot.setTaskType(taskType);
        snapshot.setTotalTimeMs(totalTimeMs);
        snapshot.setResultSummary(resultSummary);

        // Per-worker task counts and avg times
        Map<String, Integer> taskCounts = new HashMap<>();
        Map<String, List<Long>> taskTimes = new HashMap<>();

        for (TaskResult result : results) {
            String wid = result.getWorkerId();
            taskCounts.merge(wid, 1, Integer::sum);
            taskTimes.computeIfAbsent(wid, k -> new ArrayList<>()).add(result.getExecutionTimeMs());
        }

        Map<String, Double> avgTimes = new HashMap<>();
        for (Map.Entry<String, List<Long>> entry : taskTimes.entrySet()) {
            double avg = entry.getValue().stream().mapToLong(Long::longValue).average().orElse(0);
            avgTimes.put(entry.getKey(), avg);
        }

        snapshot.setPerWorkerTaskCount(taskCounts);
        snapshot.setPerWorkerAvgTime(avgTimes);

        // Load standard deviation
        double mean = taskCounts.values().stream().mapToInt(Integer::intValue).average().orElse(0);
        double variance = taskCounts.values().stream()
                .mapToDouble(c -> Math.pow(c - mean, 2))
                .average().orElse(0);
        snapshot.setLoadStdDev(Math.sqrt(variance));

        // Max node utilization
        double maxCpu = workers.stream().mapToDouble(NodeInfo::getCpuUsage).max().orElse(0);
        snapshot.setMaxNodeUtilization(maxCpu * 100);

        // Throughput
        if (totalTimeMs > 0) {
            snapshot.setThroughput((double) results.size() / (totalTimeMs / 1000.0));
        }

        // Store in history
        String key = algorithmName + "|" + taskType.name();
        history.computeIfAbsent(key, k -> new ArrayList<>()).add(snapshot);

        return snapshot;
    }

    /**
     * Get all historical snapshots.
     */
    public Map<String, List<MetricSnapshot>> getHistory() {
        return history;
    }

    /**
     * Get all snapshots as a flat list.
     */
    public List<MetricSnapshot> getAllSnapshots() {
        List<MetricSnapshot> all = new ArrayList<>();
        history.values().forEach(all::addAll);
        return all;
    }

    public void stop() {
        if (heartbeatScheduler != null) {
            heartbeatScheduler.shutdown();
        }
    }
}
