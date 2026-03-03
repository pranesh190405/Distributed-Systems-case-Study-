package com.distributed.computation;

import com.distributed.model.TaskChunk;
import com.distributed.model.TaskResult;

import java.io.*;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Prime Factorization engine.
 * Factorizes a batch of large numbers using trial division + Pollard's Rho algorithm.
 *
 * Data format (input):
 *   - int: count of numbers
 *   - long[]: the numbers to factorize
 *
 * Data format (output):
 *   - int: count of numbers
 *   - For each number:
 *     - long: the original number
 *     - int: number of prime factors
 *     - long[]: the prime factors (in ascending order, with multiplicity)
 */
public class PrimeFactorizationEngine implements ComputationEngine {

    private static final BigInteger TWO = BigInteger.valueOf(2);

    @Override
    public TaskResult execute(TaskChunk chunk, String workerId) {
        long startTime = System.currentTimeMillis();
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(chunk.getData()));
            int count = dis.readInt();

            long[] numbers = new long[count];
            for (int i = 0; i < count; i++) {
                numbers[i] = dis.readLong();
            }

            // Factorize each number
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeInt(count);

            for (int i = 0; i < count; i++) {
                long n = numbers[i];
                List<Long> factors = factorize(n);
                dos.writeLong(n);
                dos.writeInt(factors.size());
                for (long f : factors) {
                    dos.writeLong(f);
                }
            }
            dos.flush();

            long elapsed = System.currentTimeMillis() - startTime;
            System.out.printf("[PrimeFactor] Worker %s: %d numbers factorized in %dms%n",
                    workerId, count, elapsed);

            return new TaskResult(chunk.getTaskId(), chunk.getChunkId(), baos.toByteArray(), elapsed, workerId);

        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - startTime;
            return new TaskResult(chunk.getTaskId(), chunk.getChunkId(), workerId,
                    "Prime factorization failed: " + e.getMessage());
        }
    }

    /**
     * Complete prime factorization of n using trial division + Pollard's Rho.
     */
    public static List<Long> factorize(long n) {
        List<Long> factors = new ArrayList<>();
        if (n <= 1) return factors;

        // Remove factors of 2
        while (n % 2 == 0) {
            factors.add(2L);
            n /= 2;
        }

        // Remove factors of 3
        while (n % 3 == 0) {
            factors.add(3L);
            n /= 3;
        }

        // Trial division for small factors (6k±1 form) up to 100000
        for (long p = 5; p <= Math.min(100000L, (long) Math.sqrt(n) + 1); ) {
            while (n % p == 0) {
                factors.add(p);
                n /= p;
            }
            p += 2;
            while (n % p == 0) {
                factors.add(p);
                n /= p;
            }
            p += 4;
        }

        // If remaining n > 1, use Pollard's Rho for large factors
        if (n > 1) {
            factorizeRho(n, factors, 0);
        }

        factors.sort(Long::compareTo);
        return factors;
    }

    /**
     * Pollard's Rho algorithm for factoring large numbers.
     * Uses BigInteger internally to prevent long overflow.
     */
    private static void factorizeRho(long n, List<Long> factors, int depth) {
        if (n <= 1) return;
        if (depth > 25) {
            // Safety: treat as prime if too deep
            factors.add(n);
            return;
        }
        if (isPrime(n)) {
            factors.add(n);
            return;
        }

        long divisor = pollardRho(n);
        if (divisor == n || divisor <= 1) {
            // Fallback: brute force for small remaining factors
            for (long p = 2; p * p <= n; p++) {
                while (n % p == 0) {
                    factors.add(p);
                    n /= p;
                }
            }
            if (n > 1) factors.add(n);
            return;
        }

        factorizeRho(divisor, factors, depth + 1);
        factorizeRho(n / divisor, factors, depth + 1);
    }

    private static long pollardRho(long n) {
        if (n % 2 == 0) return 2;
        if (n < 4) return n;

        BigInteger N = BigInteger.valueOf(n);

        // Try multiple random starting points
        for (int attempt = 0; attempt < 20; attempt++) {
            long x = 2 + ThreadLocalRandom.current().nextLong(Math.max(1, n - 3));
            long y = x;
            long c = 1 + ThreadLocalRandom.current().nextLong(Math.max(1, n - 1));
            long d = 1;

            BigInteger X = BigInteger.valueOf(x);
            BigInteger Y = BigInteger.valueOf(y);
            BigInteger C = BigInteger.valueOf(c);

            int iterations = 0;
            int maxIterations = 1_000_000;

            while (d == 1 && iterations < maxIterations) {
                // x = (x*x + c) mod n — using BigInteger to avoid overflow
                X = X.multiply(X).add(C).mod(N);
                Y = Y.multiply(Y).add(C).mod(N);
                Y = Y.multiply(Y).add(C).mod(N);

                d = X.subtract(Y).abs().gcd(N).longValueExact();
                iterations++;
            }

            if (d != 1 && d != n) {
                return d;
            }
        }

        // Failed to find a factor — return n as fallback
        return n;
    }

    /**
     * Miller-Rabin primality test using BigInteger to avoid overflow.
     */
    private static boolean isPrime(long n) {
        if (n < 2) return false;
        if (n < 4) return true;
        if (n % 2 == 0 || n % 3 == 0) return false;

        // Quick check for small primes
        if (n < 1000) {
            for (long i = 5; i * i <= n; i += 2) {
                if (n % i == 0) return false;
            }
            return true;
        }

        BigInteger N = BigInteger.valueOf(n);
        BigInteger nMinusOne = N.subtract(BigInteger.ONE);

        // Factor out powers of 2 from n-1
        long d = n - 1;
        int r = 0;
        while (d % 2 == 0) {
            d /= 2;
            r++;
        }
        BigInteger D = BigInteger.valueOf(d);

        // Deterministic witnesses for n < 3.3×10^24
        long[] witnesses = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37};
        for (long a : witnesses) {
            if (a >= n) continue;

            BigInteger A = BigInteger.valueOf(a);
            BigInteger x = A.modPow(D, N);

            if (x.equals(BigInteger.ONE) || x.equals(nMinusOne)) continue;

            boolean found = false;
            for (int i = 0; i < r - 1; i++) {
                x = x.modPow(TWO, N);
                if (x.equals(nMinusOne)) {
                    found = true;
                    break;
                }
            }
            if (!found) return false;
        }
        return true;
    }

    private static long gcd(long a, long b) {
        while (b != 0) {
            long t = b;
            b = a % b;
            a = t;
        }
        return a;
    }
}
