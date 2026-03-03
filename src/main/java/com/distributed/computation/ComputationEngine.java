package com.distributed.computation;

import com.distributed.model.TaskChunk;
import com.distributed.model.TaskResult;

/**
 * Interface for computation engines that process task chunks on worker nodes.
 */
public interface ComputationEngine {
    TaskResult execute(TaskChunk chunk, String workerId);
}
