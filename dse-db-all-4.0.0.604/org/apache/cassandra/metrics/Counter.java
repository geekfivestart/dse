package org.apache.cassandra.metrics;

import com.codahale.metrics.Counting;
import com.codahale.metrics.Metric;

public abstract class Counter extends com.codahale.metrics.Counter implements Metric, Counting, Composable<Counter> {
   public Counter() {
   }

   public void inc() {
      this.inc(1L);
   }

   public abstract void inc(long var1);

   public void dec() {
      this.dec(1L);
   }

   public abstract void dec(long var1);

   public static Counter make(boolean isComposite) {
      return (Counter)(isComposite?new CompositeCounter():new SingleCounter());
   }
}
