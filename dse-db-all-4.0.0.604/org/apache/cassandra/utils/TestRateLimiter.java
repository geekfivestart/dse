package org.apache.cassandra.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import java.util.concurrent.atomic.AtomicReference;
import org.jboss.byteman.rule.Rule;
import org.jboss.byteman.rule.helper.Helper;

@VisibleForTesting
public class TestRateLimiter extends Helper {
   private static final AtomicReference<RateLimiter> ref = new AtomicReference();

   protected TestRateLimiter(Rule rule) {
      super(rule);
   }

   public void acquire(double rate) {
      RateLimiter limiter = (RateLimiter)ref.get();
      if(limiter == null || limiter.getRate() != rate) {
         ref.compareAndSet(limiter, RateLimiter.create(rate));
         limiter = (RateLimiter)ref.get();
      }

      limiter.acquire(1);
   }
}
