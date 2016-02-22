package com.facebook.hbase;

import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Recorder;
import org.apache.hadoop.hbase.util.FastLongHistogram;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.lib.MutableHistogram;
import org.apache.hadoop.metrics2.lib.MutableTimeHistogram;
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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Measurement(iterations = 1, time = 30, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.MINUTES)
@Timeout(time = 15, timeUnit = TimeUnit.MINUTES)
@Fork(value = 1, jvmArgsPrepend = "-server")
public class HistogramBenchmark {

  public static final int NUM_THREADS = 8;

  @State(Scope.Benchmark)
  public static class NormalMutableTimeHistogram {
    MetricHistogram histogram = new MutableTimeHistogram("","");
  }

  @State(Scope.Benchmark)
  public static class Yammer {
    com.codahale.metrics.MetricRegistry registry = new com.codahale.metrics.MetricRegistry();
    com.codahale.metrics.Histogram histo = registry.histogram("test");
  }

  @State(Scope.Benchmark)
  public static class HDRConcurrent {
    ConcurrentHistogram histo = new ConcurrentHistogram(1,TimeUnit.MINUTES.toMillis(2), 3);
  }

  @State(Scope.Benchmark)
  public static class HDRAtomic {
    Recorder histo = new Recorder(TimeUnit.HOURS.toMillis(3), 3);
  }

  @State(Scope.Benchmark)
  public static class FastLong {
    FastLongHistogram fastLongHistogram = new FastLongHistogram(50);
  }



  private long getTime() {
    return (long) (ThreadLocalRandom.current().nextGaussian() * 10 + 100);
  }


  @Benchmark
  @GroupThreads(NUM_THREADS)
  public long testMutableTimeHistogram(NormalMutableTimeHistogram histo) {
    long time = getTime();
    histo.histogram.add(time);
    return time;
  }

  @Benchmark
  @GroupThreads(NUM_THREADS)
  public long fastLong(FastLong hist) {
    long time = getTime();
    hist.fastLongHistogram.add(time, 1);
    return time;
  }

  @Benchmark
  @GroupThreads(NUM_THREADS)
  public long testYammer(Yammer hist) {
    long time = getTime();
    hist.histo.update(time);
    return time;
  }


  @Benchmark
  @GroupThreads(NUM_THREADS)
  public long testHDRConcurrent(HDRConcurrent hist) {
    long time = getTime();
    hist.histo.recordValue(time);
    return time;
  }

  @Benchmark
  @GroupThreads(NUM_THREADS)
  public long testHDRAtomic(HDRAtomic hist) {
    long time = getTime();
    hist.histo.recordValue(time);
    return time;
  }

}
