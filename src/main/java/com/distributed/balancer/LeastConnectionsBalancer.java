package com.distributed.balancer;

import com.distributed.model.NodeInfo;
import com.distributed.model.TaskChunk;

import java.util.Comparator;
import java.util.List;

/**
 * Least Connections load balancer.
 * Sends the next task to the worker with the fewest currently active tasks.
 */
public class LeastConnectionsBalancer implements LoadBalancer {

    @Override
    public NodeInfo selectWorker(List<NodeInfo> nodes, TaskChunk chunk) {
        return nodes.stream()
                .filter(NodeInfo::isAlive)
                .min(Comparator.comparingInt(NodeInfo::getActiveTasks))
                .orElseThrow(() -> new IllegalStateException("No alive workers available"));
    }

    @Override
    public void reset() {
        // No state to reset
    }

    @Override
    public String getName() {
        return "Least Connections";
    }
}
