package org.apache.cassandra.index.sasi.utils.trie;

public class Tries {
   public Tries() {
   }

   static boolean isOutOfBoundsIndex(int bitIndex) {
      return bitIndex == -3;
   }

   static boolean isEqualBitKey(int bitIndex) {
      return bitIndex == -2;
   }

   static boolean isNullBitKey(int bitIndex) {
      return bitIndex == -1;
   }

   static boolean isValidBitIndex(int bitIndex) {
      return 0 <= bitIndex;
   }

   static boolean areEqual(Object a, Object b) {
      return a == null?b == null:a.equals(b);
   }

   static <T> T notNull(T o, String message) {
      if(o == null) {
         throw new NullPointerException(message);
      } else {
         return o;
      }
   }

   static <K> K cast(Object key) {
      return (K)key;
   }
}
