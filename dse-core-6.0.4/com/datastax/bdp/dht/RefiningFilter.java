package com.datastax.bdp.dht;

import com.google.common.base.Predicate;

public abstract class RefiningFilter<T> implements Predicate<T> {
   public RefiningFilter() {
   }

   public T refine(T match) {
      return match;
   }
}
