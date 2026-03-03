package com.distributed.computation;

import com.distributed.model.TaskChunk;
import com.distributed.model.TaskResult;

import java.io.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Monte Carlo Pi Estimation engine.
 * Generates N random (x,y) points in [0,1)×[0,1) and counts how many fall inside the unit circle.
 *
 * Data format (input):
 *   - long: number of samples to generate
 *
 * Data format (output):
 *   - long: number of samples generated
 *   - long: number of points inside the unit circle (x²+y² ≤ 1)
 */
public class MonteCarloEngine implements ComputationEngine {

    @Override
    public TaskResult execute(TaskChunk chunk, String workerId) {
        long startTime = System.currentTimeMillis();
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(chunk.getData()));
            long samples = dis.readLong();

            long insideCircle = 0;
            ThreadLocalRandom rng = ThreadLocalRandom.current();

            for (long i = 0; i < samples; i++) {
                double x = rng.nextDouble();
                double y = rng.nextDouble();
                if (x * x + y * y <= 1.0) {
                    insideCircle++;
                }
            }

            // Serialize result
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeLong(samples);
            dos.writeLong(insideCircle);
            dos.flush();

            long elapsed = System.currentTimeMillis() - startTime;
            double localPi = 4.0 * insideCircle / samples;
            System.out.printf("[MonteCarlo] Worker %s: %,d samples, local π≈%.6f in %dms%n",
                    workerId, samples, localPi, elapsed);

            return new TaskResult(chunk.getTaskId(), chunk.getChunkId(), baos.toByteArray(), elapsed, workerId);

        } catch (Exception e) {
            return new TaskResult(chunk.getTaskId(), chunk.getChunkId(), workerId,
                    "Monte Carlo computation failed: " + e.getMessage());
        }
    }
}
