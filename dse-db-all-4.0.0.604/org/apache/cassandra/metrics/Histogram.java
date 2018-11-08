package org.apache.cassandra.metrics;

import com.codahale.metrics.Snapshot;
import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.config.DatabaseDescriptor;

public abstract class Histogram extends com.codahale.metrics.Histogram implements Composable<Histogram> {
   public static boolean DEFAULT_ZERO_CONSIDERATION = false;
   public static long DEFAULT_MAX_TRACKABLE_VALUE = 158329674399744L;

   protected Histogram() {
      super((com.codahale.metrics.Reservoir)null);
   }

   public abstract void update(long var1);

   public abstract long getCount();

   public abstract Snapshot getSnapshot();

   @VisibleForTesting
   public abstract void clear();

   @VisibleForTesting
   public abstract void aggregate();

   public abstract boolean considerZeroes();

   public abstract long maxTrackableValue();

   public abstract long[] getOffsets();

   public static Histogram make(boolean isComposite) {
      return make(DEFAULT_ZERO_CONSIDERATION, isComposite);
   }

   public static Histogram make(boolean considerZeroes, boolean isComposite) {
      return make(considerZeroes, DEFAULT_MAX_TRACKABLE_VALUE, isComposite);
   }

   public static Histogram make(boolean considerZeroes, long maxTrackableValue, boolean isComposite) {
      return make(considerZeroes, maxTrackableValue, DatabaseDescriptor.getMetricsHistogramUpdateTimeMillis(), isComposite);
   }

   public static Histogram make(boolean considerZeroes, long maxTrackableValue, int updateIntervalMillis, boolean isComposite) {
      return (Histogram)(isComposite?new CompositeHistogram(DecayingEstimatedHistogram.makeCompositeReservoir(considerZeroes, maxTrackableValue, updateIntervalMillis, ApproximateClock.defaultClock())):new DecayingEstimatedHistogram(considerZeroes, maxTrackableValue, updateIntervalMillis, ApproximateClock.defaultClock()));
   }

   interface Reservoir {
      boolean considerZeroes();

      long maxTrackableValue();

      long getCount();

      Snapshot getSnapshot();

      void add(Histogram var1);

      long[] getOffsets();
   }
}
