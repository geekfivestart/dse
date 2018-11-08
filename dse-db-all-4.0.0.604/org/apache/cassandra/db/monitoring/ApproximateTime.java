package org.apache.cassandra.db.monitoring;

import java.util.concurrent.TimeUnit;
import org.apache.cassandra.concurrent.ParkedThreadsMonitor;

public class ApproximateTime {
   private static final long intialCurrentTimeMillis = System.currentTimeMillis();
   private static final long initialNanoTime = System.nanoTime();
   private static volatile long currentNanoTime;
   private static final long precisionMicros = 200L;

   public ApproximateTime() {
   }

   public static void tick() {
      currentNanoTime = System.nanoTime();
   }

   public static long currentTimeMillis() {
      return intialCurrentTimeMillis + TimeUnit.MILLISECONDS.convert(currentNanoTime - initialNanoTime, TimeUnit.NANOSECONDS);
   }

   public static long nanoTime() {
      return currentNanoTime;
   }

   public static long precision() {
      return TimeUnit.MILLISECONDS.convert(200L, TimeUnit.MICROSECONDS);
   }

   static {
      currentNanoTime = initialNanoTime;
      ((ParkedThreadsMonitor)ParkedThreadsMonitor.instance.get()).addAction(ApproximateTime::tick);
   }
}
