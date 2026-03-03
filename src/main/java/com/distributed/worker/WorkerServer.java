package com.distributed.worker;

import com.distributed.computation.*;
import com.distributed.model.TaskChunk;
import com.distributed.model.TaskResult;
import com.distributed.model.TaskType;
import com.distributed.network.HeartbeatMessage;
import com.distributed.network.NetworkProtocol;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Worker node that listens for task chunks from the Master.
 * Runs as a standalone process.
 *
 * Usage: java WorkerServer <port> [weight]
 */
public class WorkerServer {

    private final int port;
    private final int weight;
    private final String workerId;
    private final ExecutorService threadPool;
    private final AtomicInteger activeTasks = new AtomicInteger(0);
    private final AtomicInteger queuedTasks = new AtomicInteger(0);
    private volatile boolean running = true;

    // Computation engines
    private final MatrixEngine matrixEngine = new MatrixEngine();
    private final MonteCarloEngine monteCarloEngine = new MonteCarloEngine();
    private final PrimeFactorizationEngine primeEngine = new PrimeFactorizationEngine();

    public WorkerServer(int port, int weight, int threadPoolSize) {
        this.port = port;
        this.weight = weight;
        this.workerId = "localhost:" + port;
        this.threadPool = Executors.newFixedThreadPool(threadPoolSize);
        System.out.printf("Worker %s initialized with weight=%d, threads=%d%n", workerId, weight, threadPoolSize);
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.printf("Worker %s listening on port %d...%n", workerId, port);

            while (running) {
                Socket clientSocket = serverSocket.accept();
                // Handle each incoming connection in a new thread
                new Thread(() -> handleConnection(clientSocket)).start();
            }
        } catch (IOException e) {
            System.err.printf("Worker %s error: %s%n", workerId, e.getMessage());
        }
    }

    private void handleConnection(Socket socket) {
        try {
            // Read the message type
            Object obj = NetworkProtocol.receiveObject(socket);

            if (obj instanceof String && obj.equals(NetworkProtocol.HEARTBEAT_REQUEST)) {
                handleHeartbeat(socket);
            } else if (obj instanceof TaskChunk) {
                handleTask((TaskChunk) obj, socket);
            } else {
                System.err.println("Unknown message type: " + obj.getClass().getName());
            }
        } catch (Exception e) {
            System.err.printf("Worker %s connection error: %s%n", workerId, e.getMessage());
        }
    }

    private void handleHeartbeat(Socket socket) throws IOException {
        Runtime runtime = Runtime.getRuntime();
        com.sun.management.OperatingSystemMXBean osBean =
                (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

        HeartbeatMessage hb = new HeartbeatMessage(
                workerId,
                activeTasks.get(),
                ((ThreadPoolExecutor) threadPool).getCorePoolSize(),
                queuedTasks.get(),
                osBean.getProcessCpuLoad(),
                runtime.freeMemory(),
                runtime.totalMemory()
        );

        NetworkProtocol.sendObject(socket, hb);
        socket.close();
    }

    private void handleTask(TaskChunk chunk, Socket socket) {
        activeTasks.incrementAndGet();
        System.out.printf("Worker %s received: %s%n", workerId, chunk);

        try {
            // Execute computation based on task type
            ComputationEngine engine = switch (chunk.getTaskType()) {
                case MATRIX_MULTIPLICATION -> matrixEngine;
                case MONTE_CARLO_PI -> monteCarloEngine;
                case PRIME_FACTORIZATION -> primeEngine;
            };

            TaskResult result = engine.execute(chunk, workerId);

            // Send result back
            NetworkProtocol.sendObject(socket, result);
            System.out.printf("Worker %s completed chunk %d in %dms%n",
                    workerId, chunk.getChunkId(), result.getExecutionTimeMs());

        } catch (Exception e) {
            try {
                TaskResult errorResult = new TaskResult(chunk.getTaskId(), chunk.getChunkId(),
                        workerId, "Execution failed: " + e.getMessage());
                NetworkProtocol.sendObject(socket, errorResult);
            } catch (IOException ioe) {
                System.err.println("Failed to send error result: " + ioe.getMessage());
            }
        } finally {
            activeTasks.decrementAndGet();
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    public void stop() {
        running = false;
        threadPool.shutdown();
    }

    public static void main(String[] args) {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8001;
        int weight = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        int threads = args.length > 2 ? Integer.parseInt(args[2]) : Runtime.getRuntime().availableProcessors();

        WorkerServer worker = new WorkerServer(port, weight, threads);

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.printf("Worker on port %d shutting down...%n", port);
            worker.stop();
        }));

        worker.start();
    }
}
