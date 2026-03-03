package com.distributed.model;

import java.io.Serializable;
import java.util.Map;

/**
 * Snapshot of metrics for one experiment run (one algorithm + one task type).
 */
public class MetricSnapshot implements Serializable {
    private static final long serialVersionUID = 1L;

    private String algorithmName;
    private TaskType taskType;
    private long totalTimeMs;
    private Map<String, Integer> perWorkerTaskCount;    // workerId -> task count
    private Map<String, Double> perWorkerAvgTime;       // workerId -> avg response time ms
    private double loadStdDev;                          // std deviation of task counts across workers
    private double maxNodeUtilization;                  // max CPU % seen
    private double throughput;                          // tasks per second
    private String resultSummary;                       // human-readable result (e.g. "π = 3.14159")

    public MetricSnapshot() {}

    public String getAlgorithmName() { return algorithmName; }
    public void setAlgorithmName(String algorithmName) { this.algorithmName = algorithmName; }

    public TaskType getTaskType() { return taskType; }
    public void setTaskType(TaskType taskType) { this.taskType = taskType; }

    public long getTotalTimeMs() { return totalTimeMs; }
    public void setTotalTimeMs(long totalTimeMs) { this.totalTimeMs = totalTimeMs; }

    public Map<String, Integer> getPerWorkerTaskCount() { return perWorkerTaskCount; }
    public void setPerWorkerTaskCount(Map<String, Integer> perWorkerTaskCount) { this.perWorkerTaskCount = perWorkerTaskCount; }

    public Map<String, Double> getPerWorkerAvgTime() { return perWorkerAvgTime; }
    public void setPerWorkerAvgTime(Map<String, Double> perWorkerAvgTime) { this.perWorkerAvgTime = perWorkerAvgTime; }

    public double getLoadStdDev() { return loadStdDev; }
    public void setLoadStdDev(double loadStdDev) { this.loadStdDev = loadStdDev; }

    public double getMaxNodeUtilization() { return maxNodeUtilization; }
    public void setMaxNodeUtilization(double maxNodeUtilization) { this.maxNodeUtilization = maxNodeUtilization; }

    public double getThroughput() { return throughput; }
    public void setThroughput(double throughput) { this.throughput = throughput; }

    public String getResultSummary() { return resultSummary; }
    public void setResultSummary(String resultSummary) { this.resultSummary = resultSummary; }

    @Override
    public String toString() {
        return String.format("MetricSnapshot[%s, %s, time=%dms, throughput=%.2f, loadDev=%.3f]",
                algorithmName, taskType, totalTimeMs, throughput, loadStdDev);
    }
}
