package com.datastax.bdp.concurrent.metrics;

import java.util.concurrent.TimeUnit;

public interface SlidingTimeStats {
   void update(long var1, TimeUnit var3);

   double computeAverage();
}
