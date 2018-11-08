package org.apache.cassandra.metrics;

import com.codahale.metrics.Snapshot;

class CompositeHistogram extends Histogram {
   private final Histogram.Reservoir reservoir;

   CompositeHistogram(Histogram.Reservoir reservoir) {
      this.reservoir = reservoir;
   }

   public boolean considerZeroes() {
      return this.reservoir.considerZeroes();
   }

   public long maxTrackableValue() {
      return this.reservoir.maxTrackableValue();
   }

   public void update(long value) {
      throw new UnsupportedOperationException("Composite histograms are read-only");
   }

   public long getCount() {
      return this.reservoir.getCount();
   }

   public Snapshot getSnapshot() {
      return this.reservoir.getSnapshot();
   }

   public long[] getOffsets() {
      return this.reservoir.getOffsets();
   }

   public void clear() {
      throw new UnsupportedOperationException("Composite histograms are read-only");
   }

   public void aggregate() {
      throw new UnsupportedOperationException("Composite histograms are read-only");
   }

   public Composable.Type getType() {
      return Composable.Type.COMPOSITE;
   }

   public void compose(Histogram histogram) {
      if(histogram == null) {
         throw new IllegalArgumentException("Histogram cannot be null");
      } else {
         this.reservoir.add(histogram);
      }
   }
}
