package org.apache.cassandra.utils.versioning;

import java.util.function.Function;

public class Versioned<V extends Enum<V> & Version<V>, T> {
   private final T[] serializers;
   private final Function<V, ? extends T> creator;

   public Versioned(Class<V> versionClass, Function<V, ? extends T> creator) {
      this.serializers = (Object[])(new Object[((Enum[])versionClass.getEnumConstants()).length]);
      this.creator = creator;
      V last = ((Enum[])versionClass.getEnumConstants())[((Enum[])versionClass.getEnumConstants()).length - 1];
      this.serializers[last.ordinal()] = creator.apply(last);
   }

   public T get(V version) {
      int ordinal = version.ordinal();
      T serializer = this.serializers[ordinal];
      return serializer == null?(this.serializers[ordinal] = this.creator.apply(version)):serializer;
   }
}
