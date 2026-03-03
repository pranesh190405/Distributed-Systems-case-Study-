package com.distributed.balancer;

import com.distributed.model.NodeInfo;
import com.distributed.model.TaskChunk;

import java.util.Comparator;
import java.util.List;

/**
 * Least Response Time load balancer.
 * Sends the next task to the worker with the lowest average response time.
 * Falls back to least connections if no response time data is available.
 */
public class LeastResponseTimeBalancer implements LoadBalancer {

    @Override
    public NodeInfo selectWorker(List<NodeInfo> nodes, TaskChunk chunk) {
        List<NodeInfo> aliveNodes = nodes.stream().filter(NodeInfo::isAlive).toList();
        if (aliveNodes.isEmpty()) {
            throw new IllegalStateException("No alive workers available");
        }

        // If no worker has completed tasks yet, fall back to least connections
        boolean anyHasData = aliveNodes.stream().anyMatch(n -> n.getCompletedTasks() > 0);

        if (!anyHasData) {
            return aliveNodes.stream()
                    .min(Comparator.comparingInt(NodeInfo::getActiveTasks))
                    .orElseThrow();
        }

        // Pick worker with lowest average response time (0 means no data, treat as best)
        return aliveNodes.stream()
                .min(Comparator.comparingDouble(n -> {
                    double avg = n.getAvgResponseTime();
                    return avg == 0 ? Double.MIN_VALUE : avg;
                }))
                .orElseThrow();
    }

    @Override
    public void reset() {
        // No internal state to reset
    }

    @Override
    public String getName() {
        return "Least Response Time";
    }
}
