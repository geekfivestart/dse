package org.apache.cassandra.net;

import com.google.common.util.concurrent.RateLimiter;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.utils.SlidingTimeRate;
import org.apache.cassandra.utils.TimeSource;
import org.apache.cassandra.utils.concurrent.IntervalLock;

class RateBasedBackPressureState extends IntervalLock implements BackPressureState {
   private final InetAddress host;
   final SlidingTimeRate incomingRate;
   final SlidingTimeRate outgoingRate;
   final RateLimiter rateLimiter;

   RateBasedBackPressureState(InetAddress host, TimeSource timeSource, long windowSize) {
      super(timeSource);
      this.host = host;
      this.incomingRate = new SlidingTimeRate(timeSource, windowSize, windowSize / 10L, TimeUnit.MILLISECONDS);
      this.outgoingRate = new SlidingTimeRate(timeSource, windowSize, windowSize / 10L, TimeUnit.MILLISECONDS);
      this.rateLimiter = RateLimiter.create(1.0D / 0.0);
   }

   public void onRequestSent(Request<?, ?> request) {
   }

   public void onResponseReceived() {
      this.readLock().lock();

      try {
         this.incomingRate.update(1);
         this.outgoingRate.update(1);
      } finally {
         this.readLock().unlock();
      }

   }

   public void onResponseTimeout() {
      this.readLock().lock();

      try {
         this.outgoingRate.update(1);
      } finally {
         this.readLock().unlock();
      }

   }

   public double getBackPressureRateLimit() {
      return this.rateLimiter.getRate();
   }

   public InetAddress getHost() {
      return this.host;
   }

   public boolean equals(Object obj) {
      if(obj instanceof RateBasedBackPressureState) {
         RateBasedBackPressureState other = (RateBasedBackPressureState)obj;
         return this.host.equals(other.host);
      } else {
         return false;
      }
   }

   public int hashCode() {
      return this.host.hashCode();
   }

   public String toString() {
      return String.format("[host: %s, incoming rate: %.3f, outgoing rate: %.3f, rate limit: %.3f]", new Object[]{this.host, Double.valueOf(this.incomingRate.get(TimeUnit.SECONDS)), Double.valueOf(this.outgoingRate.get(TimeUnit.SECONDS)), Double.valueOf(this.rateLimiter.getRate())});
   }
}
