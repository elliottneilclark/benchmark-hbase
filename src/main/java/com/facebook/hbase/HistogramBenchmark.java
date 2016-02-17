package com.facebook.hbase;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import org.HdrHistogram.AtomicHistogram;
import org.HdrHistogram.ConcurrentHistogram;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.lib.MetricMutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableHistogram;
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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
@Timeout(time = 15, timeUnit = TimeUnit.MINUTES)
@Fork(value = 1, jvmArgsPrepend = "-server")
public class HistogramBenchmark {

  public static final int NUM_THREADS = 1;

  @State(Scope.Benchmark)
  public static class NormalHisto {
    MetricHistogram histogram = new MutableHistogram("","");
  }

  @State(Scope.Benchmark)
  public static class Quant {
    MetricMutableQuantiles quant = new MetricMutableQuantiles("t", "t", "t", "t", 60);
  }

  @State(Scope.Benchmark)
  public static class Yammer {
    com.yammer.metrics.core.MetricsRegistry registry = new com.yammer.metrics.core.MetricsRegistry();
    Histogram histo = registry.newHistogram(new MetricName("t","t","t"),true);
  }

  @State(Scope.Benchmark)
  public static class HDRConcurrent {
    ConcurrentHistogram histo = new ConcurrentHistogram(1,TimeUnit.MINUTES.toMillis(2), 3);
  }

  @State(Scope.Benchmark)
  public static class HDRAtomic {
    AtomicHistogram histo = new AtomicHistogram(1,TimeUnit.MINUTES.toMillis(2), 3);
  }
  private long getTime() {
    return ThreadLocalRandom.current().nextLong(3000);
  }


  @Benchmark
  @GroupThreads(NUM_THREADS)
  public long testNormal(NormalHisto histo) {
    long time = getTime();
    histo.histogram.add(time);
    return time;
  }

  @Benchmark
  @GroupThreads(NUM_THREADS)
  public long testQuant(Quant hist) {
    long time = getTime();
    hist.quant.add(time);
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
