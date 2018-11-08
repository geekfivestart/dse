package com.datastax.bdp.cassandra.metrics;

import com.datastax.bdp.util.QuantileEstimator;
import java.util.SortedMap;
import java.util.TreeMap;

public class LatencyValues {
   private static final int VALUE_ARRAY_LENGTH = 4;
   private static final int LATENCY_VALUE_INDEX = 0;
   private static final int OPERATION_VALUE_INDEX = 1;
   long[] values;
   QuantileEstimator[] estimators;

   public LatencyValues() {
      this(false);
   }

   public LatencyValues(boolean quantiles) {
      this.values = new long[4];
      if(quantiles) {
         this.estimators = new QuantileEstimator[2];

         for(int i = 0; i < this.estimators.length; ++i) {
            this.estimators[i] = new QuantileEstimator();
         }
      } else {
         this.estimators = null;
      }

   }

   public void increment(long inc, LatencyValues.EventType type) {
      this.values[0 + type.offset] += inc;
      ++this.values[1 + type.offset];
      if(this.estimators != null) {
         this.estimators[type.offset / 2].update((float)inc);
      }

   }

   public void reset(LatencyValues.EventType type) {
      this.set(0L, 0L, type);
      if(this.estimators != null) {
         this.estimators[type.offset / 2] = new QuantileEstimator();
      }

   }

   public void set(long value, long count, LatencyValues.EventType type) {
      this.values[0 + type.offset] = value;
      this.values[1 + type.offset] = count;
   }

   public long getValue(LatencyValues.EventType type) {
      return this.values[0 + type.offset];
   }

   public long getCount(LatencyValues.EventType type) {
      return this.values[1 + type.offset];
   }

   public SortedMap<Double, Double> getQuantiles(LatencyValues.EventType type) {
      return (SortedMap)(this.estimators == null?new TreeMap():this.estimators[type.offset / 2].getQuantiles());
   }

   public static enum EventType {
      READ(0),
      WRITE(2);

      final int offset;

      private EventType(int offset) {
         this.offset = offset;
      }
   }
}
