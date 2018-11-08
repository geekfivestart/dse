package org.apache.cassandra.metrics;

import org.apache.cassandra.utils.concurrent.LongAdder;

class SingleCounter extends Counter {
   private final LongAdder count = new LongAdder();

   SingleCounter() {
   }

   public void inc(long n) {
      this.count.add(n);
   }

   public void dec(long n) {
      this.count.add(-n);
   }

   public long getCount() {
      return this.count.sum();
   }

   public Composable.Type getType() {
      return Composable.Type.SINGLE;
   }
}
