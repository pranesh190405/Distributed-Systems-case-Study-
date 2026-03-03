package com.distributed.balancer;

import com.distributed.model.NodeInfo;
import com.distributed.model.TaskChunk;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Weighted Round Robin load balancer.
 * Builds an expanded list where higher-weight workers appear more frequently.
 * A worker with weight=3 gets 3x more tasks than weight=1.
 */
public class WeightedRoundRobinBalancer implements LoadBalancer {

    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public NodeInfo selectWorker(List<NodeInfo> nodes, TaskChunk chunk) {
        List<NodeInfo> aliveNodes = nodes.stream().filter(NodeInfo::isAlive).toList();
        if (aliveNodes.isEmpty()) {
            throw new IllegalStateException("No alive workers available");
        }

        // Build expanded list based on weights
        List<NodeInfo> expanded = new ArrayList<>();
        for (NodeInfo node : aliveNodes) {
            int w = Math.max(1, node.getWeight());
            for (int i = 0; i < w; i++) {
                expanded.add(node);
            }
        }

        int index = Math.abs(counter.getAndIncrement()) % expanded.size();
        return expanded.get(index);
    }

    @Override
    public void reset() {
        counter.set(0);
    }

    @Override
    public String getName() {
        return "Weighted Round Robin";
    }
}
