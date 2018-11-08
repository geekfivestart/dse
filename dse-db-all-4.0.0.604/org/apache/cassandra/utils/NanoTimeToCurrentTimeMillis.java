package org.apache.cassandra.utils;

import java.util.concurrent.TimeUnit;
import org.apache.cassandra.concurrent.ScheduledExecutors;

public class NanoTimeToCurrentTimeMillis {
   private static final String TIMESTAMP_UPDATE_INTERVAL_PROPERTY = "cassandra.NANOTIMETOMILLIS_TIMESTAMP_UPDATE_INTERVAL";
   private static final long TIMESTAMP_UPDATE_INTERVAL = Long.getLong("cassandra.NANOTIMETOMILLIS_TIMESTAMP_UPDATE_INTERVAL", 10000L).longValue();
   private static volatile long[] TIMESTAMP_BASE = new long[]{System.currentTimeMillis(), System.nanoTime()};

   public NanoTimeToCurrentTimeMillis() {
   }

   public static long convert(long nanoTime) {
      long[] timestampBase = TIMESTAMP_BASE;
      return timestampBase[0] + TimeUnit.NANOSECONDS.toMillis(nanoTime - timestampBase[1]);
   }

   public static void updateNow() {
      ScheduledExecutors.scheduledFastTasks.submit(NanoTimeToCurrentTimeMillis::updateTimestampBase);
   }

   private static void updateTimestampBase() {
      TIMESTAMP_BASE = new long[]{Math.max(TIMESTAMP_BASE[0], System.currentTimeMillis()), Math.max(TIMESTAMP_BASE[1], System.nanoTime())};
   }

   static {
      ScheduledExecutors.scheduledFastTasks.scheduleWithFixedDelay(NanoTimeToCurrentTimeMillis::updateTimestampBase, TIMESTAMP_UPDATE_INTERVAL, TIMESTAMP_UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
   }
}
