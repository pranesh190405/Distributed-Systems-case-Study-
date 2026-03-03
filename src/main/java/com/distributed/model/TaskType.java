package com.distributed.model;

public enum TaskType {
    MATRIX_MULTIPLICATION("Matrix Multiplication"),
    MONTE_CARLO_PI("Monte Carlo Pi Estimation"),
    PRIME_FACTORIZATION("Prime Factorization");

    private final String displayName;

    TaskType(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String toString() {
        return displayName;
    }
}
