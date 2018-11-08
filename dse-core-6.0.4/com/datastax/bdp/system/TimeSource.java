package com.datastax.bdp.system;

import java.util.concurrent.TimeUnit;

public interface TimeSource {
   long currentTimeMillis();

   long nanoTime();

   TimeSource sleepUninterruptibly(long var1, TimeUnit var3);

   TimeSource sleep(long var1, TimeUnit var3) throws InterruptedException;
}
