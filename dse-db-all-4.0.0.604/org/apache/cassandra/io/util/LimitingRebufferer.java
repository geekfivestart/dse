package org.apache.cassandra.io.util;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.RateLimiter;

public class LimitingRebufferer extends WrappingRebufferer {
   private final RateLimiter limiter;
   private final int limitQuant;

   LimitingRebufferer(Rebufferer wrapped, RateLimiter limiter, int limitQuant) {
      super(wrapped);
      this.limiter = limiter;
      this.limitQuant = limitQuant;
   }

   public Rebufferer.BufferHolder rebuffer(long position, Rebufferer.ReaderConstraint rc) {
      WrappingRebufferer.WrappingBufferHolder ret = (WrappingRebufferer.WrappingBufferHolder)super.rebuffer(position, rc);
      int posInBuffer = Ints.checkedCast(position - ret.offset());
      int remaining = ret.limit() - posInBuffer;
      if(remaining == 0) {
         return ret;
      } else {
         if(remaining > this.limitQuant) {
            ret.limit(posInBuffer + this.limitQuant);
            remaining = this.limitQuant;
         }

         this.limiter.acquire(remaining);
         return ret;
      }
   }

   protected String paramsToString() {
      return this.limiter.toString();
   }
}
