package com.distributed.master;

import com.distributed.balancer.*;
import com.distributed.metrics.MetricsCollector;
import com.distributed.model.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Main orchestration class for the Master node.
 * Coordinates task splitting, load balancing, dispatching, and result assembly.
 */
public class MasterServer {

    private final List<NodeInfo> workers;
    private final Map<String, LoadBalancer> balancers;
    private final TaskSplitter splitter;
    private final ResultAssembler assembler;
    private final TaskDispatcher dispatcher;
    private final MetricsCollector metricsCollector;

    // Config values
    private int matrixSize;
    private long monteCarloSamples;
    private int primeCount;
    private long primeMin;
    private long primeMax;
    private int chunksCount;

    private Consumer<String> logCallback;

    public MasterServer() {
        this.workers = new ArrayList<>();
        this.balancers = new LinkedHashMap<>();
        this.splitter = new TaskSplitter();
        this.assembler = new ResultAssembler();
        this.dispatcher = new TaskDispatcher(20, 120000);
        this.metricsCollector = new MetricsCollector(workers);

        // Register all load balancers
        registerBalancer(new RoundRobinBalancer());
        registerBalancer(new WeightedRoundRobinBalancer());
        registerBalancer(new LeastConnectionsBalancer());
        registerBalancer(new LeastResponseTimeBalancer());
        registerBalancer(new RandomBalancer());
    }

    private void registerBalancer(LoadBalancer lb) {
        balancers.put(lb.getName(), lb);
    }

    /**
     * Load configuration from properties file.
     */
    public void loadConfig(String configPath) throws IOException {
        Properties props = new Properties();
        // Try classpath first, then filesystem
        InputStream is = getClass().getClassLoader().getResourceAsStream(configPath);
        if (is == null) {
            is = new FileInputStream(configPath);
        }
        props.load(is);
        is.close();

        // Parse workers
        String workersStr = props.getProperty("workers", "localhost:8001:1,localhost:8002:1,localhost:8003:1");
        for (String w : workersStr.split(",")) {
            String[] parts = w.trim().split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            int weight = parts.length > 2 ? Integer.parseInt(parts[2]) : 1;
            workers.add(new NodeInfo(host, port, weight));
        }

        // Parse task params
        matrixSize = Integer.parseInt(props.getProperty("matrix.size", "500"));
        monteCarloSamples = Long.parseLong(props.getProperty("montecarlo.samples", "10000000"));
        primeCount = Integer.parseInt(props.getProperty("prime.count", "500"));
        primeMin = Long.parseLong(props.getProperty("prime.min", "100000000000"));
        primeMax = Long.parseLong(props.getProperty("prime.max", "999999999999999"));
        chunksCount = Integer.parseInt(props.getProperty("chunks.count", "12"));

        log("Configuration loaded: " + workers.size() + " workers, matrix=" + matrixSize +
                ", samples=" + monteCarloSamples + ", primes=" + primeCount + ", chunks=" + chunksCount);
    }

    /**
     * Initialize — check worker connections.
     */
    public void initialize() {
        log("Checking worker connections...");
        metricsCollector.checkAllWorkers();
        long aliveCount = workers.stream().filter(NodeInfo::isAlive).count();
        log(aliveCount + "/" + workers.size() + " workers online.");

        // Start heartbeat monitoring
        metricsCollector.startHeartbeatMonitoring(3000);
    }

    /**
     * Run a single experiment: one task type with one algorithm.
     */
    public MetricSnapshot runExperiment(TaskType taskType, String algorithmName) {
        LoadBalancer balancer = balancers.get(algorithmName);
        if (balancer == null) {
            throw new IllegalArgumentException("Unknown algorithm: " + algorithmName);
        }

        log("=== Starting Experiment: " + taskType.getDisplayName() + " with " + algorithmName + " ===");

        // Reset stats
        balancer.reset();
        workers.forEach(NodeInfo::resetStats);

        // Check workers are alive
        metricsCollector.checkAllWorkers();
        List<NodeInfo> aliveWorkers = workers.stream().filter(NodeInfo::isAlive).toList();
        if (aliveWorkers.isEmpty()) {
            throw new IllegalStateException("No workers are online!");
        }
        log("Active workers: " + aliveWorkers.size());

        // Adjust chunks to not exceed alive workers if needed
        int effectiveChunks = Math.min(chunksCount, aliveWorkers.size() * 4);

        // 1. Split the task
        log("Splitting task into " + effectiveChunks + " chunks...");
        String taskId = UUID.randomUUID().toString().substring(0, 8);

        TaskSplitter.SplitResult splitResult = switch (taskType) {
            case MATRIX_MULTIPLICATION -> splitter.splitMatrixMultiplication(matrixSize, effectiveChunks, taskId);
            case MONTE_CARLO_PI -> splitter.splitMonteCarlo(monteCarloSamples, effectiveChunks, taskId);
            case PRIME_FACTORIZATION -> splitter.splitPrimeFactorization(primeCount, primeMin, primeMax, effectiveChunks, taskId);
        };

        List<TaskChunk> chunks = splitResult.getChunks();
        log("Created " + chunks.size() + " chunks.");

        // 2. Dispatch chunks via load balancer
        long startTime = System.currentTimeMillis();
        List<CompletableFuture<TaskResult>> futures = new ArrayList<>();

        for (TaskChunk chunk : chunks) {
            NodeInfo selectedWorker = balancer.selectWorker(workers, chunk);
            log(String.format("  Chunk %d → %s", chunk.getChunkId(), selectedWorker.getId()));
            futures.add(dispatcher.dispatch(chunk, selectedWorker));
        }

        // 3. Wait for all results
        log("Waiting for " + futures.size() + " results...");
        List<TaskResult> results = new ArrayList<>();
        for (CompletableFuture<TaskResult> future : futures) {
            try {
                TaskResult result = future.get(120, TimeUnit.SECONDS);
                results.add(result);
                log(String.format("  ✓ Chunk %d from %s (%dms)",
                        result.getChunkId(), result.getWorkerId(), result.getExecutionTimeMs()));
            } catch (Exception e) {
                log("  ✗ Chunk failed: " + e.getMessage());
            }
        }

        long totalTime = System.currentTimeMillis() - startTime;
        log(String.format("All chunks completed in %dms", totalTime));

        // 4. Assemble results
        String resultSummary = switch (taskType) {
            case MATRIX_MULTIPLICATION -> assembler.assembleMatrixResult(results, splitResult);
            case MONTE_CARLO_PI -> assembler.assembleMonteCarloResult(results, splitResult);
            case PRIME_FACTORIZATION -> assembler.assemblePrimeResult(results, splitResult);
        };

        log("\n--- Result ---\n" + resultSummary);

        // 5. Build metrics snapshot
        MetricSnapshot snapshot = metricsCollector.buildSnapshot(
                algorithmName, taskType, totalTime, results, resultSummary);

        // Compute accuracy
        double accuracy = assembler.getAccuracy(taskType, resultSummary);

        log(String.format("=== Experiment Complete: %s × %s — %dms, throughput=%.2f tasks/s ===\n",
                taskType.getDisplayName(), algorithmName, totalTime, snapshot.getThroughput()));

        return snapshot;
    }

    // --- Getters ---
    public List<NodeInfo> getWorkers() { return workers; }
    public Map<String, LoadBalancer> getBalancers() { return balancers; }
    public MetricsCollector getMetricsCollector() { return metricsCollector; }
    public int getMatrixSize() { return matrixSize; }
    public long getMonteCarloSamples() { return monteCarloSamples; }
    public int getPrimeCount() { return primeCount; }
    public int getChunksCount() { return chunksCount; }

    public void setLogCallback(Consumer<String> callback) {
        this.logCallback = callback;
    }

    private void log(String msg) {
        System.out.println("[Master] " + msg);
        if (logCallback != null) {
            logCallback.accept(msg);
        }
    }

    public void shutdown() {
        metricsCollector.stop();
        dispatcher.shutdown();
    }
}
