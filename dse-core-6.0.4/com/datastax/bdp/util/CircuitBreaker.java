package com.datastax.bdp.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CircuitBreaker {
   private static final Logger logger = LoggerFactory.getLogger(CircuitBreaker.class);
   private CircuitBreaker.State state;
   private long nextCheck;
   private int failureRate;
   private int threshold;
   private int timeoutInSecs;

   public CircuitBreaker(int threshold, int timeoutInSecs) {
      this.state = CircuitBreaker.State.CLOSED;
      this.nextCheck = 0L;
      this.failureRate = 0;
      this.threshold = 1;
      this.timeoutInSecs = 1;
      if(threshold > 0) {
         this.threshold = threshold;
      }

      if(timeoutInSecs > 0) {
         this.timeoutInSecs = timeoutInSecs;
      }

   }

   public boolean allow() {
      if(this.state == CircuitBreaker.State.OPEN && this.nextCheck < System.currentTimeMillis() / 1000L) {
         if(logger.isDebugEnabled()) {
            logger.debug("allow:  going half-open");
         }

         this.state = CircuitBreaker.State.HALF_OPEN;
      }

      return this.state == CircuitBreaker.State.CLOSED || this.state == CircuitBreaker.State.HALF_OPEN;
   }

   public void success() {
      if(this.state == CircuitBreaker.State.HALF_OPEN) {
         this.reset();
      }

   }

   public void failure() {
      if(this.state == CircuitBreaker.State.HALF_OPEN) {
         if(logger.isDebugEnabled()) {
            logger.debug("failure: in half-open, trip");
         }

         this.trip();
      } else {
         ++this.failureRate;
         if(this.failureRate >= this.threshold) {
            if(logger.isDebugEnabled()) {
               logger.debug("failure: reached threash, tripped");
            }

            this.trip();
         }
      }

   }

   private void reset() {
      this.state = CircuitBreaker.State.CLOSED;
      this.failureRate = 0;
   }

   private void trip() {
      if(this.state != CircuitBreaker.State.OPEN) {
         if(logger.isDebugEnabled()) {
            logger.debug("trip: tripped");
         }

         this.state = CircuitBreaker.State.OPEN;
         this.nextCheck = System.currentTimeMillis() / 1000L + (long)this.timeoutInSecs;
      }

   }

   private static enum State {
      CLOSED,
      HALF_OPEN,
      OPEN;

      private State() {
      }
   }
}
