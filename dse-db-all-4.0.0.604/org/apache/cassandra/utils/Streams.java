package org.apache.cassandra.utils;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class Streams {
   private Streams() {
   }

   public static <T> Stream<T> of(Iterable<T> iterable) {
      return StreamSupport.stream(iterable.spliterator(), false);
   }
}
