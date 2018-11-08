package org.apache.cassandra.utils;

import org.apache.cassandra.utils.concurrent.SharedCloseable;

public interface IFilter extends SharedCloseable {
   void add(IFilter.FilterKey var1);

   boolean isPresent(IFilter.FilterKey var1);

   void clear();

   long serializedSize();

   void close();

   IFilter sharedCopy();

   long offHeapSize();

   public interface FilterKey {
      void filterHash(long[] var1);

      default short filterHashLowerBits() {
         long[] dest = new long[2];
         this.filterHash(dest);
         return (short)((int)dest[1]);
      }
   }
}
