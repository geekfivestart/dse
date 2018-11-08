package org.apache.cassandra.utils.concurrent;

public interface RefCounted<T> {
   Ref<T> tryRef();

   Ref<T> ref();

   public interface Tidy {
      void tidy() throws Exception;

      String name();
   }
}
