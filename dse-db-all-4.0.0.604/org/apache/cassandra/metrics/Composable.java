package org.apache.cassandra.metrics;

import com.codahale.metrics.Metric;

public interface Composable<M extends Metric> {
   Composable.Type getType();

   default void compose(M metric) {
      if(!null.$assertionsDisabled && this.getType() != Composable.Type.SINGLE) {
         throw new AssertionError("Composite metrics should implement compose()");
      } else {
         throw new UnsupportedOperationException("Single metric cannot be composed with other metrics");
      }
   }

   static default {
      if(null.$assertionsDisabled) {
         ;
      }

   }

   public static enum Type {
      SINGLE,
      COMPOSITE;

      private Type() {
      }
   }
}
