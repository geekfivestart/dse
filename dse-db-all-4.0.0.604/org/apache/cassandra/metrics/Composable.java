package org.apache.cassandra.metrics;

import com.codahale.metrics.Metric;

public interface Composable<M extends Metric> {
   Composable.Type getType();

   default void compose(M metric) {
      assert this.getType() == Type.SINGLE : "Composite metrics should implement compose()";
      throw new UnsupportedOperationException("Single metric cannot be composed with other metrics");
   }


   public static enum Type {
      SINGLE,
      COMPOSITE;

      private Type() {
      }
   }
}
