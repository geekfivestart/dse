package com.datastax.bdp.concurrent;

import java.util.concurrent.TimeUnit;

public interface Task {
   int run();

   boolean await(long var1, TimeUnit var3) throws InterruptedException;

   void await() throws InterruptedException;

   void signal();

   long getEpoch();

   void setEpoch(long var1);

   long getTimestampNanos();
}
