package com.distributed.master;

import com.distributed.model.TaskChunk;
import com.distributed.model.TaskType;

import java.io.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Splits large computational problems into chunks for distribution across workers.
 */
public class TaskSplitter {

    /**
     * Split a matrix multiplication task into row-slab chunks.
     * Generates two random N×N matrices, splits A by rows.
     *
     * @param matrixSize N for N×N matrices
     * @param numChunks  Number of chunks to split into
     * @param taskId     Unique task identifier
     * @return List of TaskChunks + the generated matrices stored internally
     */
    public SplitResult splitMatrixMultiplication(int matrixSize, int numChunks, String taskId) {
        int n = matrixSize;
        double[][] matA = new double[n][n];
        double[][] matB = new double[n][n];
        Random rng = new Random(42); // Fixed seed for reproducibility

        // Generate random matrices
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                matA[i][j] = rng.nextDouble() * 10;
                matB[i][j] = rng.nextDouble() * 10;
            }
        }

        List<TaskChunk> chunks = new ArrayList<>();
        int rowsPerChunk = n / numChunks;
        int remainder = n % numChunks;

        int startRow = 0;
        for (int c = 0; c < numChunks; c++) {
            int chunkRows = rowsPerChunk + (c < remainder ? 1 : 0);
            int endRow = startRow + chunkRows;

            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos);
                dos.writeInt(startRow);
                dos.writeInt(endRow);
                dos.writeInt(n);

                // Write slab of A
                for (int i = startRow; i < endRow; i++) {
                    for (int j = 0; j < n; j++) {
                        dos.writeDouble(matA[i][j]);
                    }
                }

                // Write full B
                for (int i = 0; i < n; i++) {
                    for (int j = 0; j < n; j++) {
                        dos.writeDouble(matB[i][j]);
                    }
                }
                dos.flush();

                chunks.add(new TaskChunk(taskId, c, numChunks, TaskType.MATRIX_MULTIPLICATION, baos.toByteArray()));
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize matrix chunk", e);
            }

            startRow = endRow;
        }

        SplitResult result = new SplitResult(chunks);
        result.setMetadata("matrixSize", n);
        result.setMetadata("matA", matA);
        result.setMetadata("matB", matB);
        return result;
    }

    /**
     * Split a Monte Carlo Pi estimation task into sample-count chunks.
     */
    public SplitResult splitMonteCarlo(long totalSamples, int numChunks, String taskId) {
        List<TaskChunk> chunks = new ArrayList<>();
        long samplesPerChunk = totalSamples / numChunks;
        long remainder = totalSamples % numChunks;

        for (int c = 0; c < numChunks; c++) {
            long samples = samplesPerChunk + (c < remainder ? 1 : 0);

            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos);
                dos.writeLong(samples);
                dos.flush();

                chunks.add(new TaskChunk(taskId, c, numChunks, TaskType.MONTE_CARLO_PI, baos.toByteArray()));
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize Monte Carlo chunk", e);
            }
        }

        SplitResult result = new SplitResult(chunks);
        result.setMetadata("totalSamples", totalSamples);
        return result;
    }

    /**
     * Split a prime factorization task into batches of numbers.
     */
    public SplitResult splitPrimeFactorization(int count, long minValue, long maxValue,
                                                int numChunks, String taskId) {
        // Generate random large numbers
        Random rng = new Random(12345);
        long[] numbers = new long[count];
        for (int i = 0; i < count; i++) {
            numbers[i] = minValue + (long) (rng.nextDouble() * (maxValue - minValue));
            // Make sure they're not trivially easy (avoid even numbers mostly)
            if (numbers[i] % 2 == 0) numbers[i]++;
        }

        List<TaskChunk> chunks = new ArrayList<>();
        int numbersPerChunk = count / numChunks;
        int remainder = count % numChunks;

        int start = 0;
        for (int c = 0; c < numChunks; c++) {
            int chunkSize = numbersPerChunk + (c < remainder ? 1 : 0);

            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos);
                dos.writeInt(chunkSize);
                for (int i = start; i < start + chunkSize; i++) {
                    dos.writeLong(numbers[i]);
                }
                dos.flush();

                chunks.add(new TaskChunk(taskId, c, numChunks, TaskType.PRIME_FACTORIZATION, baos.toByteArray()));
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize prime chunk", e);
            }

            start += chunkSize;
        }

        SplitResult result = new SplitResult(chunks);
        result.setMetadata("totalNumbers", count);
        result.setMetadata("numbers", numbers);
        return result;
    }

    /**
     * Container for the result of a split operation.
     * Holds the chunks plus any metadata needed for result assembly.
     */
    public static class SplitResult {
        private final List<TaskChunk> chunks;
        private final Map<String, Object> metadata = new HashMap<>();

        public SplitResult(List<TaskChunk> chunks) {
            this.chunks = chunks;
        }

        public List<TaskChunk> getChunks() { return chunks; }

        public void setMetadata(String key, Object value) { metadata.put(key, value); }

        @SuppressWarnings("unchecked")
        public <T> T getMetadata(String key) { return (T) metadata.get(key); }
    }
}
