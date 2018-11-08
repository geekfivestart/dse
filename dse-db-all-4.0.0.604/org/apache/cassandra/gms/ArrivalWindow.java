package org.apache.cassandra.gms;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ArrivalWindow {
   private static final Logger logger = LoggerFactory.getLogger(ArrivalWindow.class);
   private long tLast = 0L;
   private final ArrayBackedBoundedStats arrivalIntervals;
   private double lastReportedPhi = 4.9E-324D;
   private final long MAX_INTERVAL_IN_NANO = getMaxInterval();

   ArrivalWindow(int size) {
      this.arrivalIntervals = new ArrayBackedBoundedStats(size);
   }

   private static long getMaxInterval() {
      String newvalue = System.getProperty("cassandra.fd_max_interval_ms");
      if(newvalue == null) {
         return FailureDetector.INITIAL_VALUE_NANOS;
      } else {
         logger.info("Overriding FD MAX_INTERVAL to {}ms", newvalue);
         return TimeUnit.NANOSECONDS.convert((long)Integer.parseInt(newvalue), TimeUnit.MILLISECONDS);
      }
   }

   synchronized void add(long value, InetAddress ep) {
      assert this.tLast >= 0L;

      if(this.tLast > 0L) {
         long interArrivalTime = value - this.tLast;
         if(interArrivalTime <= this.MAX_INTERVAL_IN_NANO) {
            this.arrivalIntervals.add(interArrivalTime);
            logger.trace("Reporting interval time of {} for {}", Long.valueOf(interArrivalTime), ep);
         } else {
            logger.trace("Ignoring interval time of {} for {}", Long.valueOf(interArrivalTime), ep);
         }
      } else {
         this.arrivalIntervals.add(FailureDetector.INITIAL_VALUE_NANOS);
      }

      this.tLast = value;
   }

   double mean() {
      return this.arrivalIntervals.mean();
   }

   double phi(long tnow) {
      assert this.arrivalIntervals.mean() > 0.0D && this.tLast > 0L;

      long t = tnow - this.tLast;
      this.lastReportedPhi = (double)t / this.mean();
      return this.lastReportedPhi;
   }

   double getLastReportedPhi() {
      return this.lastReportedPhi;
   }

   public String toString() {
      return Arrays.toString(this.arrivalIntervals.getArrivalIntervals());
   }
}
