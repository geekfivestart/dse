package org.apache.cassandra.utils;

import java.util.Comparator;

public class Comparables {
   public Comparables() {
   }

   public static <T extends Comparable<? super T>> T max(T a, T b) {
      return a.compareTo(b) < 0?b:a;
   }

   public static <T> T max(T a, T b, Comparator<T> comparator) {
      return comparator.compare(a, b) < 0?b:a;
   }

   public static <T extends Comparable<? super T>> T min(T a, T b) {
      return a.compareTo(b) > 0?b:a;
   }

   public static <T> T min(T a, T b, Comparator<T> comparator) {
      return comparator.compare(a, b) > 0?b:a;
   }
}
