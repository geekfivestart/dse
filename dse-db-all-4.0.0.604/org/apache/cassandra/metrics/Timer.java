package org.apache.cassandra.metrics;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Metered;
import com.codahale.metrics.Sampling;
import com.codahale.metrics.Snapshot;
import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class Timer extends com.codahale.metrics.Timer implements Metered, Sampling, Composable<Timer> {
   private final Meter meter;
   private final Histogram histogram;
   private final Composable.Type composableType;
   private final Clock clock;

   public Timer() {
      this(false);
   }

   public Timer(boolean isComposite) {
      this(Histogram.make(isComposite), Clock.defaultClock(), isComposite);
   }

   public Timer(Histogram histogram, Clock clock, boolean isComposite) {
      this.meter = new Meter(clock, Counter.make(isComposite));
      this.histogram = histogram;
      this.composableType = isComposite?Composable.Type.COMPOSITE:Composable.Type.SINGLE;
      this.clock = clock;
   }

   public void update(long duration, TimeUnit unit) {
      this.update(unit.toNanos(duration));
   }

   public <T> T time(Callable<T> event) throws Exception {
      if(this.composableType == Composable.Type.COMPOSITE) {
         throw new UnsupportedOperationException("Composite timer cannot time an event");
      } else {
         long startTime = this.clock.getTick();

         Object var4;
         try {
            var4 = event.call();
         } finally {
            this.update(this.clock.getTick() - startTime);
         }

         return (T)var4;
      }
   }

   public com.codahale.metrics.Timer.Context time() {
      throw new UnsupportedOperationException("Use Timer.timer() instead.");
   }

   public Timer.Context timer() {
      if(this.composableType == Composable.Type.COMPOSITE) {
         throw new UnsupportedOperationException("Composite timer cannot time an event");
      } else {
         return new Timer.Context(this, this.clock);
      }
   }

   public long getCount() {
      return this.histogram.getCount();
   }

   public double getFifteenMinuteRate() {
      return this.meter.getFifteenMinuteRate();
   }

   public double getFiveMinuteRate() {
      return this.meter.getFiveMinuteRate();
   }

   public double getMeanRate() {
      return this.meter.getMeanRate();
   }

   public double getOneMinuteRate() {
      return this.meter.getOneMinuteRate();
   }

   public Snapshot getSnapshot() {
      return this.histogram.getSnapshot();
   }

   private void update(long duration) {
      if(this.composableType == Composable.Type.COMPOSITE) {
         throw new UnsupportedOperationException("Composite timer cannot be updated");
      } else {
         if(duration >= 0L) {
            this.histogram.update(duration);
            this.meter.mark();
         }

      }
   }

   public Composable.Type getType() {
      return this.composableType;
   }

   public void compose(Timer metric) {
      if(this.composableType != Composable.Type.COMPOSITE) {
         throw new UnsupportedOperationException("Non composite timer cannot be composed with another timer");
      } else {
         this.meter.compose(metric.meter);
         this.histogram.compose(metric.histogram);
      }
   }

   @VisibleForTesting
   public Histogram getHistogram() {
      return this.histogram;
   }

   public String toString() {
      Snapshot snapshot = this.histogram.getSnapshot();
      return String.format("min: %d, avg: %.2f, max: %d, [p50: %.2f, p75: %.2f, p95: %.2f, p99: %.2f]", new Object[]{Long.valueOf(snapshot.getMin()), Double.valueOf(snapshot.getMean()), Long.valueOf(snapshot.getMax()), Double.valueOf(snapshot.getMedian()), Double.valueOf(snapshot.get75thPercentile()), Double.valueOf(snapshot.get95thPercentile()), Double.valueOf(snapshot.get99thPercentile())});
   }

   public static class Context implements Closeable {
      private final Timer timer;
      private final Clock clock;
      private final long startTime;

      private Context(Timer timer, Clock clock) {
         this.timer = timer;
         this.clock = clock;
         this.startTime = clock.getTick();
      }

      public long stop() {
         long elapsed = this.clock.getTick() - this.startTime;
         this.timer.update(elapsed, TimeUnit.NANOSECONDS);
         return elapsed;
      }

      public void close() {
         this.stop();
      }
   }
}
