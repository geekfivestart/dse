package org.apache.cassandra.cache;

public interface CacheSize {
   long capacity();

   void setCapacity(long var1);

   int size();

   long weightedSize();
}
