package org.apache.cassandra.utils;

public interface Flags {
   public static boolean isEmpty(int flags) {
      return flags == 0;
   }

   public static boolean containsAll(int flags, int testFlags) {
      return (flags & testFlags) == testFlags;
   }

   public static boolean contains(int flags, int testFlags) {
      return (flags & testFlags) != 0;
   }

   public static int add(int flags, int toAdd) {
      return flags | toAdd;
   }

   public static int remove(int flags, int toRemove) {
      return flags & ~toRemove;
   }
}
