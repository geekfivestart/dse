package com.datastax.bdp.util.schema;

import com.google.common.base.MoreObjects;

public class InconsistentValue<T> {
   public final T expected;
   public final T current;

   public InconsistentValue(T expected, T current) {
      this.expected = expected;
      this.current = current;
   }

   public String toString() {
      return MoreObjects.toStringHelper(this).add("expected", this.expected).add("current", this.current).toString();
   }
}
