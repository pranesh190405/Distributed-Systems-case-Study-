package com.distributed.master;

import com.distributed.model.TaskResult;
import com.distributed.model.TaskType;

import java.io.*;
import java.util.*;

/**
 * Assembles partial results from workers into a final answer.
 */
public class ResultAssembler {

    /**
     * Assemble matrix multiplication results.
     * Returns a human-readable summary (verifying against serial computation for correctness).
     */
    public String assembleMatrixResult(List<TaskResult> results, TaskSplitter.SplitResult splitResult) {
        int n = splitResult.getMetadata("matrixSize");
        double[][] matA = splitResult.getMetadata("matA");
        double[][] matB = splitResult.getMetadata("matB");

        // Sort results by chunk ID
        results.sort(Comparator.comparingInt(TaskResult::getChunkId));

        // Reconstruct result matrix
        double[][] resultMatrix = new double[n][n];
        for (TaskResult result : results) {
            if (!result.isSuccess()) {
                return "ERROR: Chunk " + result.getChunkId() + " failed: " + result.getErrorMessage();
            }
            try {
                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(result.getData()));
                int startRow = dis.readInt();
                int endRow = dis.readInt();
                int matSize = dis.readInt();
                for (int i = startRow; i < endRow; i++) {
                    for (int j = 0; j < matSize; j++) {
                        resultMatrix[i][j] = dis.readDouble();
                    }
                }
            } catch (IOException e) {
                return "ERROR: Failed to deserialize chunk " + result.getChunkId();
            }
        }

        // Verify a sample against serial computation
        boolean verified = verifyMatrixSample(matA, matB, resultMatrix, n);

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Matrix %d×%d multiplication complete.%n", n, n));
        sb.append(String.format("Result matrix C[0][0] = %.4f%n", resultMatrix[0][0]));
        sb.append(String.format("Result matrix C[%d][%d] = %.4f%n", n - 1, n - 1, resultMatrix[n - 1][n - 1]));
        sb.append(String.format("Verification: %s", verified ? "✓ PASSED" : "✗ FAILED"));
        return sb.toString();
    }

    private boolean verifyMatrixSample(double[][] a, double[][] b, double[][] c, int n) {
        // Verify a few cells
        int[] checkRows = {0, n / 2, n - 1};
        int[] checkCols = {0, n / 2, n - 1};

        for (int row : checkRows) {
            for (int col : checkCols) {
                double expected = 0;
                for (int k = 0; k < n; k++) {
                    expected += a[row][k] * b[k][col];
                }
                if (Math.abs(expected - c[row][col]) > 1e-6) {
                    System.err.printf("Verification failed at [%d][%d]: expected=%.6f, got=%.6f%n",
                            row, col, expected, c[row][col]);
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Assemble Monte Carlo Pi results.
     */
    public String assembleMonteCarloResult(List<TaskResult> results, TaskSplitter.SplitResult splitResult) {
        long totalSamples = 0;
        long totalInside = 0;

        for (TaskResult result : results) {
            if (!result.isSuccess()) {
                return "ERROR: Chunk " + result.getChunkId() + " failed: " + result.getErrorMessage();
            }
            try {
                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(result.getData()));
                long samples = dis.readLong();
                long inside = dis.readLong();
                totalSamples += samples;
                totalInside += inside;
            } catch (IOException e) {
                return "ERROR: Failed to deserialize chunk " + result.getChunkId();
            }
        }

        double pi = 4.0 * totalInside / totalSamples;
        double error = Math.abs(pi - Math.PI);

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Monte Carlo Pi Estimation%n"));
        sb.append(String.format("Total samples: %,d%n", totalSamples));
        sb.append(String.format("Points inside circle: %,d%n", totalInside));
        sb.append(String.format("π ≈ %.10f%n", pi));
        sb.append(String.format("Actual π = %.10f%n", Math.PI));
        sb.append(String.format("Error: %.10f (%.6f%%)", error, (error / Math.PI) * 100));
        return sb.toString();
    }

    /**
     * Assemble prime factorization results.
     */
    public String assemblePrimeResult(List<TaskResult> results, TaskSplitter.SplitResult splitResult) {
        results.sort(Comparator.comparingInt(TaskResult::getChunkId));

        int totalFactorized = 0;
        int verified = 0;

        StringBuilder sampleOutput = new StringBuilder();
        boolean showSamples = true;
        int sampleCount = 0;

        for (TaskResult result : results) {
            if (!result.isSuccess()) {
                return "ERROR: Chunk " + result.getChunkId() + " failed: " + result.getErrorMessage();
            }
            try {
                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(result.getData()));
                int count = dis.readInt();
                totalFactorized += count;

                for (int i = 0; i < count; i++) {
                    long number = dis.readLong();
                    int numFactors = dis.readInt();
                    List<Long> factors = new ArrayList<>();
                    for (int j = 0; j < numFactors; j++) {
                        factors.add(dis.readLong());
                    }

                    // Verify: product of factors should equal original number
                    long product = 1;
                    for (long f : factors) product *= f;
                    if (product == number) verified++;

                    // Show first 5 as samples
                    if (showSamples && sampleCount < 5) {
                        sampleOutput.append(String.format("  %,d = %s%n", number, factors));
                        sampleCount++;
                    }
                    if (sampleCount >= 5) showSamples = false;
                }
            } catch (IOException e) {
                return "ERROR: Failed to deserialize chunk " + result.getChunkId();
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Prime Factorization Complete%n"));
        sb.append(String.format("Numbers factorized: %,d%n", totalFactorized));
        sb.append(String.format("Verified correct: %,d / %,d%n", verified, totalFactorized));
        sb.append(String.format("Sample results:%n%s", sampleOutput));
        return sb.toString();
    }

    /**
     * Get result accuracy for the comparison table.
     */
    public double getAccuracy(TaskType taskType, String resultSummary) {
        return switch (taskType) {
            case MONTE_CARLO_PI -> {
                // Extract the pi value
                try {
                    String[] lines = resultSummary.split("\n");
                    for (String line : lines) {
                        if (line.contains("π ≈")) {
                            double pi = Double.parseDouble(line.split("≈")[1].trim());
                            yield 100.0 * (1.0 - Math.abs(pi - Math.PI) / Math.PI);
                        }
                    }
                } catch (Exception ignored) {}
                yield 0.0;
            }
            case MATRIX_MULTIPLICATION -> resultSummary.contains("PASSED") ? 100.0 : 0.0;
            case PRIME_FACTORIZATION -> {
                try {
                    String[] lines = resultSummary.split("\n");
                    for (String line : lines) {
                        if (line.contains("Verified correct:")) {
                            String[] parts = line.split(":")[1].trim().split("/");
                            double v = Double.parseDouble(parts[0].trim().replace(",", ""));
                            double t = Double.parseDouble(parts[1].trim().replace(",", ""));
                            yield (v / t) * 100.0;
                        }
                    }
                } catch (Exception ignored) {}
                yield 0.0;
            }
        };
    }
}
