package com.datastax.bdp.dht.endpoint;

import java.util.Comparator;
import java.util.Random;

public final class SeededComparator<T> implements Comparator<T> {
   private static final Random generator = new Random(System.currentTimeMillis());
   private final Comparator<T> delegate;
   private final int seed;

   public SeededComparator(Comparator<T> delegate) {
      this(delegate, generator.nextInt());
   }

   public SeededComparator(Comparator<T> delegate, int seed) {
      this.delegate = delegate;
      this.seed = seed;
   }

   public int compare(T first, T second) {
      return this.delegate.compare(first, second);
   }

   public int getSeed() {
      return this.seed;
   }
}
