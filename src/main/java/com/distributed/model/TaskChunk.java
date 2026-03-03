package com.distributed.model;

import java.io.Serializable;

/**
 * Represents a chunk of work sent from Master to a Worker.
 */
public class TaskChunk implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String taskId;
    private final int chunkId;
    private final int totalChunks;
    private final TaskType taskType;
    private final byte[] data;
    private final long timestamp;

    public TaskChunk(String taskId, int chunkId, int totalChunks, TaskType taskType, byte[] data) {
        this.taskId = taskId;
        this.chunkId = chunkId;
        this.totalChunks = totalChunks;
        this.taskType = taskType;
        this.data = data;
        this.timestamp = System.currentTimeMillis();
    }

    public String getTaskId() { return taskId; }
    public int getChunkId() { return chunkId; }
    public int getTotalChunks() { return totalChunks; }
    public TaskType getTaskType() { return taskType; }
    public byte[] getData() { return data; }
    public long getTimestamp() { return timestamp; }

    @Override
    public String toString() {
        return String.format("TaskChunk[id=%s, chunk=%d/%d, type=%s, dataSize=%d]",
                taskId, chunkId, totalChunks, taskType, data != null ? data.length : 0);
    }
}
