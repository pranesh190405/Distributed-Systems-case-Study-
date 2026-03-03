package com.distributed.balancer;

import com.distributed.model.NodeInfo;
import com.distributed.model.TaskChunk;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Random load balancer.
 * Randomly picks a worker for each task. Baseline/control algorithm.
 */
public class RandomBalancer implements LoadBalancer {

    @Override
    public NodeInfo selectWorker(List<NodeInfo> nodes, TaskChunk chunk) {
        List<NodeInfo> aliveNodes = nodes.stream().filter(NodeInfo::isAlive).toList();
        if (aliveNodes.isEmpty()) {
            throw new IllegalStateException("No alive workers available");
        }
        int index = ThreadLocalRandom.current().nextInt(aliveNodes.size());
        return aliveNodes.get(index);
    }

    @Override
    public void reset() {
        // No state to reset
    }

    @Override
    public String getName() {
        return "Random";
    }
}
