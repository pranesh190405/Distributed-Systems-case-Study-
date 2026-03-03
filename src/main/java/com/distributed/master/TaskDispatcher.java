package com.distributed.master;

import com.distributed.model.NodeInfo;
import com.distributed.model.TaskChunk;
import com.distributed.model.TaskResult;
import com.distributed.network.NetworkProtocol;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.*;

/**
 * Dispatches task chunks to worker nodes over TCP and collects results.
 */
public class TaskDispatcher {

    private final ExecutorService dispatchPool;
    private final int timeoutMs;

    public TaskDispatcher(int poolSize, int timeoutMs) {
        this.dispatchPool = Executors.newFixedThreadPool(poolSize);
        this.timeoutMs = timeoutMs;
    }

    /**
     * Send a task chunk to a specific worker and return the result asynchronously.
     */
    public CompletableFuture<TaskResult> dispatch(TaskChunk chunk, NodeInfo worker) {
        return CompletableFuture.supplyAsync(() -> {
            worker.incrementActiveTasks();
            long startTime = System.currentTimeMillis();

            try {
                Socket socket = new Socket(worker.getHost(), worker.getPort());
                socket.setSoTimeout(timeoutMs);

                // Send task chunk
                NetworkProtocol.sendObject(socket, chunk);

                // Receive result
                Object response = NetworkProtocol.receiveObject(socket);
                socket.close();

                if (response instanceof TaskResult result) {
                    long elapsed = System.currentTimeMillis() - startTime;
                    worker.recordTaskCompletion(elapsed);
                    return result;
                } else {
                    throw new RuntimeException("Unexpected response type: " + response.getClass());
                }

            } catch (Exception e) {
                System.err.printf("Dispatch to %s failed for chunk %d: %s%n",
                        worker.getId(), chunk.getChunkId(), e.getMessage());
                return new TaskResult(chunk.getTaskId(), chunk.getChunkId(),
                        worker.getId(), "Dispatch failed: " + e.getMessage());
            } finally {
                worker.decrementActiveTasks();
            }
        }, dispatchPool);
    }

    public void shutdown() {
        dispatchPool.shutdown();
    }
}
