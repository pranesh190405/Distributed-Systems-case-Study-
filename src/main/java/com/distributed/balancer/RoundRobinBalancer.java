package com.distributed.balancer;

import com.distributed.model.NodeInfo;
import com.distributed.model.TaskChunk;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Round Robin load balancer.
 * Assigns tasks cyclically to workers regardless of their current state.
 */
public class RoundRobinBalancer implements LoadBalancer {

    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public NodeInfo selectWorker(List<NodeInfo> nodes, TaskChunk chunk) {
        List<NodeInfo> aliveNodes = nodes.stream().filter(NodeInfo::isAlive).toList();
        if (aliveNodes.isEmpty()) {
            throw new IllegalStateException("No alive workers available");
        }
        int index = Math.abs(counter.getAndIncrement()) % aliveNodes.size();
        return aliveNodes.get(index);
    }

    @Override
    public void reset() {
        counter.set(0);
    }

    @Override
    public String getName() {
        return "Round Robin";
    }
}
