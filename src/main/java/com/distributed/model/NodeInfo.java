package com.distributed.model;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Describes a worker node and its current state.
 */
public class NodeInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String id;
    private final String host;
    private final int port;
    private final int weight;

    // Mutable state — updated by metrics collector
    private volatile boolean alive;
    private final AtomicInteger activeTasks = new AtomicInteger(0);
    private final AtomicLong totalResponseTime = new AtomicLong(0);
    private final AtomicInteger completedTasks = new AtomicInteger(0);
    private volatile double cpuUsage;
    private volatile long memoryUsage;

    public NodeInfo(String host, int port, int weight) {
        this.id = host + ":" + port;
        this.host = host;
        this.port = port;
        this.weight = weight;
        this.alive = false;
    }

    public String getId() { return id; }
    public String getHost() { return host; }
    public int getPort() { return port; }
    public int getWeight() { return weight; }
    public boolean isAlive() { return alive; }
    public void setAlive(boolean alive) { this.alive = alive; }

    public int getActiveTasks() { return activeTasks.get(); }
    public void incrementActiveTasks() { activeTasks.incrementAndGet(); }
    public void decrementActiveTasks() { activeTasks.decrementAndGet(); }

    public void recordTaskCompletion(long responseTimeMs) {
        totalResponseTime.addAndGet(responseTimeMs);
        completedTasks.incrementAndGet();
    }

    public double getAvgResponseTime() {
        int completed = completedTasks.get();
        if (completed == 0) return 0;
        return (double) totalResponseTime.get() / completed;
    }

    public int getCompletedTasks() { return completedTasks.get(); }

    public double getCpuUsage() { return cpuUsage; }
    public void setCpuUsage(double cpuUsage) { this.cpuUsage = cpuUsage; }

    public long getMemoryUsage() { return memoryUsage; }
    public void setMemoryUsage(long memoryUsage) { this.memoryUsage = memoryUsage; }

    public void resetStats() {
        activeTasks.set(0);
        totalResponseTime.set(0);
        completedTasks.set(0);
        cpuUsage = 0;
        memoryUsage = 0;
    }

    @Override
    public String toString() {
        return String.format("NodeInfo[%s, weight=%d, alive=%b, active=%d, avg=%.1fms]",
                id, weight, alive, activeTasks.get(), getAvgResponseTime());
    }
}
