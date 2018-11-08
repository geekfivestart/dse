package org.apache.cassandra.io.util;

public interface BytesReadTracker {
   long getBytesRead();

   void reset(long var1);
}
