package org.apache.cassandra.utils;

import java.util.concurrent.TimeUnit;

public interface TimeSource {
   long currentTimeMillis();

   default int currentTimeSeconds() {
      return (int)(this.currentTimeMillis() / 1000L);
   }

   long nanoTime();

   TimeSource sleepUninterruptibly(long var1, TimeUnit var3);

   TimeSource sleep(long var1, TimeUnit var3) throws InterruptedException;
}
