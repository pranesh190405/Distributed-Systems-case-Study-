package com.distributed.balancer;

import com.distributed.model.NodeInfo;
import com.distributed.model.TaskChunk;

import java.util.List;

/**
 * Interface for load balancing algorithms.
 * Each algorithm decides which worker should receive the next task chunk.
 */
public interface LoadBalancer {

    /**
     * Select a worker node for the given task chunk.
     * @param nodes List of all available worker nodes
     * @param chunk The task chunk to assign
     * @return The selected worker node
     */
    NodeInfo selectWorker(List<NodeInfo> nodes, TaskChunk chunk);

    /**
     * Reset internal state (called between experiment runs).
     */
    void reset();

    /**
     * Get the display name of this algorithm.
     */
    String getName();
}
