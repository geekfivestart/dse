package org.apache.cassandra.utils;

import com.codahale.metrics.Snapshot;
import java.io.OutputStream;
import java.util.function.Supplier;
import org.HdrHistogram.AbstractHistogram;

class HistogramSnapshot extends Snapshot {
   private final AbstractHistogram histogram;

   public HistogramSnapshot(Supplier<AbstractHistogram> histogram) {
      this.histogram = ((AbstractHistogram)histogram.get()).copy();
   }

   public double getValue(double quantile) {
      return (double)this.histogram.getValueAtPercentile(quantile * 100.0D);
   }

   public long[] getValues() {
      return HistogramUtil.estimatedHistogramFromHdrHistogram(this.histogram).getBuckets(false);
   }

   public int size() {
      return (int)Math.min(2147483647L, this.histogram.getTotalCount());
   }

   public long getMax() {
      return this.histogram.getMaxValue();
   }

   public double getMean() {
      return this.histogram.getMean();
   }

   public long getMin() {
      return this.histogram.getMinValue();
   }

   public double getStdDev() {
      return this.histogram.getStdDeviation();
   }

   public void dump(OutputStream output) {
      throw new UnsupportedOperationException();
   }

   public long getTotalCount() {
      return this.histogram.getTotalCount();
   }
}
