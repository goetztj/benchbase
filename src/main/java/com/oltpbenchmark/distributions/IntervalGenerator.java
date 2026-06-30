package com.oltpbenchmark.distributions;

import java.util.Random;

/** BenchBase implementation of the IntervalGenerator logic. */
public class IntervalGenerator extends IntegerGenerator {

  private final Random random;
  private final int distance;
  private final int stepSize;
  private final int max;
  private long transactionCounter = 0;

  public IntervalGenerator(int steps, int distance, int max) {
    this.random = new Random();
    this.distance = distance;
    this.stepSize = steps;
    this.max = max;
  }

  @Override
  public int nextInt() {
    // In BenchBase workers, you'd typically increment a counter per call
    // or pass the actual transaction ID if available.
    return nextInt(transactionCounter++);
  }

  /** Overloaded to allow passing a specific sequence ID from the Worker. */
  @Override
  public int nextInt(long txnId) {
    int offset = (distance > 0) ? (random.nextInt(distance)) : 0;

    // Match your YCSB logic: ((txnId * step) + offset) % max
    int next = (int) (((txnId * stepSize) + offset) % max);

    // CRITICAL: IntegerGenerator requires this to keep lastInt() working
    setLastInt(next);

    return next;
  }

  @Override
  public double mean() {
    // Standard YCSB/BenchBase practice for non-stationary distributions
    return -1;
  }
}
