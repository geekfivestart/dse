package org.apache.cassandra.metrics;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BinaryOperator;
import java.util.function.Function;

class CompositeCounter extends Counter {
   private final CopyOnWriteArrayList<Counter> childCounters = new CopyOnWriteArrayList();

   CompositeCounter() {
   }

   public void inc(long n) {
      throw new UnsupportedOperationException("Composite counters are read-only");
   }

   public void dec(long n) {
      throw new UnsupportedOperationException("Composite counters are read-only");
   }

   public long getCount() {
      return ((Long)this.childCounters.stream().map(com.codahale.metrics.Counter::getCount).reduce(Long.valueOf(0L), Long::sum)).longValue();
   }

   public Composable.Type getType() {
      return Composable.Type.COMPOSITE;
   }

   public void compose(Counter metric) {
      this.childCounters.add(metric);
   }
}
