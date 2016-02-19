package com.facebook.hbase;



import org.apache.hadoop.hbase.util.Counter;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.MINUTES)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.MINUTES)
@Timeout(time = 15, timeUnit = TimeUnit.MINUTES)
@Fork(value = 1, jvmArgsPrepend = "-server")
public class IncrementBenchmark {
  public static final int NUM_THREADS = 64;


  @State(Scope.Benchmark)
  public static class AtomicLongTest {
    AtomicLong al = new AtomicLong();
  }

  @State(Scope.Benchmark)
  public static class CounterTest {
    Counter count = new Counter();
  }

  @State(Scope.Benchmark)
  public static class AddrTest {
    LongAdder adder = new LongAdder();
  }

  @Benchmark
  @GroupThreads(NUM_THREADS)
  public long testAtomicLong(AtomicLongTest al) {
    return al.al.incrementAndGet();
  }

  @Benchmark
  @GroupThreads(NUM_THREADS)
  public void testCounter(CounterTest count) {
    count.count.increment();
  }

  @Benchmark
  @GroupThreads(NUM_THREADS)
  public void testLongAdder(AddrTest addr) {
    addr.adder.increment();
  }
}
