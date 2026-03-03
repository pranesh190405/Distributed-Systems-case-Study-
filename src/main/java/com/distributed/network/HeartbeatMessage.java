package com.distributed.network;

import java.io.Serializable;

/**
 * Heartbeat message exchanged between Master and Worker for health monitoring.
 */
public class HeartbeatMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String workerId;
    private final int activeTasks;
    private final int threadPoolSize;
    private final int queueLength;
    private final double cpuUsage;
    private final long freeMemory;
    private final long totalMemory;
    private final boolean alive;

    public HeartbeatMessage(String workerId, int activeTasks, int threadPoolSize,
                            int queueLength, double cpuUsage, long freeMemory, long totalMemory) {
        this.workerId = workerId;
        this.activeTasks = activeTasks;
        this.threadPoolSize = threadPoolSize;
        this.queueLength = queueLength;
        this.cpuUsage = cpuUsage;
        this.freeMemory = freeMemory;
        this.totalMemory = totalMemory;
        this.alive = true;
    }

    public String getWorkerId() { return workerId; }
    public int getActiveTasks() { return activeTasks; }
    public int getThreadPoolSize() { return threadPoolSize; }
    public int getQueueLength() { return queueLength; }
    public double getCpuUsage() { return cpuUsage; }
    public long getFreeMemory() { return freeMemory; }
    public long getTotalMemory() { return totalMemory; }
    public boolean isAlive() { return alive; }

    @Override
    public String toString() {
        return String.format("Heartbeat[%s, active=%d, cpu=%.1f%%, mem=%dMB/%dMB]",
                workerId, activeTasks, cpuUsage * 100, freeMemory / (1024 * 1024), totalMemory / (1024 * 1024));
    }
}
