package org.apache.cassandra.utils;

public interface Flags {
   static default boolean isEmpty(int flags) {
      return flags == 0;
   }

   static default boolean containsAll(int flags, int testFlags) {
      return (flags & testFlags) == testFlags;
   }

   static default boolean contains(int flags, int testFlags) {
      return (flags & testFlags) != 0;
   }

   static default int add(int flags, int toAdd) {
      return flags | toAdd;
   }

   static default int remove(int flags, int toRemove) {
      return flags & ~toRemove;
   }
}
