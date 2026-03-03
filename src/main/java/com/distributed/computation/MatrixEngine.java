package com.distributed.computation;

import com.distributed.model.TaskChunk;
import com.distributed.model.TaskResult;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Matrix Multiplication engine.
 * Receives a row-slab of Matrix A + full Matrix B, computes partial result C.
 *
 * Data format (input):
 *   - int: startRow
 *   - int: endRow (exclusive)
 *   - int: matrixSize (N — both matrices are N×N)
 *   - double[]: slab of A (rows startRow..endRow-1, each N doubles)
 *   - double[]: full Matrix B (N×N doubles)
 *
 * Data format (output):
 *   - int: startRow
 *   - int: endRow
 *   - int: matrixSize
 *   - double[]: result slab C (rows startRow..endRow-1, each N doubles)
 */
public class MatrixEngine implements ComputationEngine {

    @Override
    public TaskResult execute(TaskChunk chunk, String workerId) {
        long startTime = System.currentTimeMillis();
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(chunk.getData()));

            int startRow = dis.readInt();
            int endRow = dis.readInt();
            int n = dis.readInt();
            int slabRows = endRow - startRow;

            // Read slab of A
            double[][] slabA = new double[slabRows][n];
            for (int i = 0; i < slabRows; i++) {
                for (int j = 0; j < n; j++) {
                    slabA[i][j] = dis.readDouble();
                }
            }

            // Read full B
            double[][] matB = new double[n][n];
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < n; j++) {
                    matB[i][j] = dis.readDouble();
                }
            }

            // Compute C slab = slabA × matB
            double[][] slabC = new double[slabRows][n];
            for (int i = 0; i < slabRows; i++) {
                for (int j = 0; j < n; j++) {
                    double sum = 0;
                    for (int k = 0; k < n; k++) {
                        sum += slabA[i][k] * matB[k][j];
                    }
                    slabC[i][j] = sum;
                }
            }

            // Serialize result
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeInt(startRow);
            dos.writeInt(endRow);
            dos.writeInt(n);
            for (int i = 0; i < slabRows; i++) {
                for (int j = 0; j < n; j++) {
                    dos.writeDouble(slabC[i][j]);
                }
            }
            dos.flush();

            long elapsed = System.currentTimeMillis() - startTime;
            System.out.printf("[Matrix] Worker %s: rows %d-%d of %dx%d in %dms%n",
                    workerId, startRow, endRow - 1, n, n, elapsed);

            return new TaskResult(chunk.getTaskId(), chunk.getChunkId(), baos.toByteArray(), elapsed, workerId);

        } catch (Exception e) {
            return new TaskResult(chunk.getTaskId(), chunk.getChunkId(), workerId,
                    "Matrix computation failed: " + e.getMessage());
        }
    }
}
