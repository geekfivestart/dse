package org.apache.cassandra.utils;

import com.codahale.metrics.Snapshot;
import java.util.function.Supplier;
import org.HdrHistogram.AbstractHistogram;
import org.apache.cassandra.metrics.Histogram;
import org.apache.cassandra.metrics.Composable.Type;

public class CMRMetricAdapter extends Histogram {
   private final Supplier<AbstractHistogram> histogram;
   private volatile HistogramSnapshot snapshot;

   public CMRMetricAdapter(Supplier<AbstractHistogram> histogram) {
      this.histogram = histogram;
      this.snapshot = new HistogramSnapshot(histogram);
   }

   public long getCount() {
      return ((AbstractHistogram)this.histogram.get()).getTotalCount();
   }

   public Snapshot getSnapshot() {
      this.maybeUpdateSnapshot();
      return this.snapshot;
   }

   public Type getType() {
      throw new UnsupportedOperationException();
   }

   public void update(long value) {
      throw new UnsupportedOperationException();
   }

   public void clear() {
      throw new UnsupportedOperationException();
   }

   public void aggregate() {
      throw new UnsupportedOperationException();
   }

   public boolean considerZeroes() {
      throw new UnsupportedOperationException();
   }

   public long maxTrackableValue() {
      throw new UnsupportedOperationException();
   }

   public long[] getOffsets() {
      throw new UnsupportedOperationException();
   }

   private void maybeUpdateSnapshot() {
      if(this.snapshot == null || this.snapshot.getTotalCount() != ((AbstractHistogram)this.histogram.get()).getTotalCount()) {
         this.snapshot = new HistogramSnapshot(this.histogram);
      }

   }
}
