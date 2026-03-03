package com.distributed.model;

import java.io.Serializable;

/**
 * Result returned by a Worker to the Master after computing a TaskChunk.
 */
public class TaskResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String taskId;
    private final int chunkId;
    private final byte[] data;
    private final long executionTimeMs;
    private final String workerId;
    private final boolean success;
    private final String errorMessage;

    public TaskResult(String taskId, int chunkId, byte[] data, long executionTimeMs, String workerId) {
        this.taskId = taskId;
        this.chunkId = chunkId;
        this.data = data;
        this.executionTimeMs = executionTimeMs;
        this.workerId = workerId;
        this.success = true;
        this.errorMessage = null;
    }

    public TaskResult(String taskId, int chunkId, String workerId, String errorMessage) {
        this.taskId = taskId;
        this.chunkId = chunkId;
        this.data = null;
        this.executionTimeMs = -1;
        this.workerId = workerId;
        this.success = false;
        this.errorMessage = errorMessage;
    }

    public String getTaskId() { return taskId; }
    public int getChunkId() { return chunkId; }
    public byte[] getData() { return data; }
    public long getExecutionTimeMs() { return executionTimeMs; }
    public String getWorkerId() { return workerId; }
    public boolean isSuccess() { return success; }
    public String getErrorMessage() { return errorMessage; }

    @Override
    public String toString() {
        return String.format("TaskResult[id=%s, chunk=%d, worker=%s, time=%dms, success=%b]",
                taskId, chunkId, workerId, executionTimeMs, success);
    }
}
