package org.apache.cassandra.net;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCRunnable;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.utils.ApproximateTimeSource;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.TimeSource;
import org.apache.cassandra.utils.concurrent.IntervalLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RateBasedBackPressure implements BackPressureStrategy<RateBasedBackPressureState> {
   static final String HIGH_RATIO = "high_ratio";
   static final String FACTOR = "factor";
   static final String FLOW = "flow";
   private static final String BACK_PRESSURE_HIGH_RATIO = "0.90";
   private static final String BACK_PRESSURE_FACTOR = "5";
   private static final String BACK_PRESSURE_FLOW = "FAST";
   private static final Logger logger = LoggerFactory.getLogger(RateBasedBackPressure.class);
   private static final NoSpamLogger tenSecsNoSpamLogger;
   private static final NoSpamLogger oneMinNoSpamLogger;
   protected final TimeSource timeSource;
   protected final double highRatio;
   protected final int factor;
   protected final RateBasedBackPressure.Flow flow;
   protected final long windowSize;
   private final Cache<Set<RateBasedBackPressureState>, RateBasedBackPressure.IntervalRateLimiter> rateLimiters;

   public static ParameterizedClass withDefaultParams() {
      return new ParameterizedClass(RateBasedBackPressure.class.getName(), ImmutableMap.of("high_ratio", "0.90", "factor", "5", "flow", "FAST"));
   }

   public RateBasedBackPressure(Map<String, Object> args) {
      this(args, new ApproximateTimeSource(), DatabaseDescriptor.getWriteRpcTimeout());
   }

   @VisibleForTesting
   public RateBasedBackPressure(Map<String, Object> args, TimeSource timeSource, long windowSize) {
      this.rateLimiters = Caffeine.newBuilder().expireAfterAccess(1L, TimeUnit.HOURS).executor(MoreExecutors.directExecutor()).build();
      if(args.size() != 3) {
         throw new IllegalArgumentException(RateBasedBackPressure.class.getCanonicalName() + " requires 3 arguments: high ratio, back-pressure factor and flow type.");
      } else {
         try {
            this.highRatio = Double.parseDouble(args.getOrDefault("high_ratio", "").toString().trim());
            this.factor = Integer.parseInt(args.getOrDefault("factor", "").toString().trim());
            this.flow = RateBasedBackPressure.Flow.valueOf(args.getOrDefault("flow", "").toString().trim().toUpperCase());
         } catch (Exception var6) {
            throw new IllegalArgumentException(var6.getMessage(), var6);
         }

         if(this.highRatio > 0.0D && this.highRatio <= 1.0D) {
            if(this.factor < 1) {
               throw new IllegalArgumentException("Back-pressure factor must be >= 1");
            } else if(windowSize < 10L) {
               throw new IllegalArgumentException("Back-pressure window size must be >= 10");
            } else {
               this.timeSource = timeSource;
               this.windowSize = windowSize;
               logger.info("Initialized back-pressure with high ratio: {}, factor: {}, flow: {}, window size: {}.", new Object[]{Double.valueOf(this.highRatio), Integer.valueOf(this.factor), this.flow, Long.valueOf(windowSize)});
            }
         } else {
            throw new IllegalArgumentException("Back-pressure high ratio must be > 0 and <= 1");
         }
      }
   }

   public CompletableFuture<Void> apply(Set<RateBasedBackPressureState> states, long timeout, TimeUnit unit) {
      boolean isUpdated = false;
      double minRateLimit = 1.0D / 0.0;
      double maxRateLimit = -1.0D / 0.0;
      double minIncomingRate = 1.0D / 0.0;
      RateLimiter currentMin = null;
      RateLimiter currentMax = null;
      Iterator var14 = states.iterator();

      while(var14.hasNext()) {
         RateBasedBackPressureState backPressure = (RateBasedBackPressureState)var14.next();
         double incomingRate = backPressure.incomingRate.get(TimeUnit.SECONDS);
         double outgoingRate = backPressure.outgoingRate.get(TimeUnit.SECONDS);
         if(incomingRate < minIncomingRate) {
            minIncomingRate = incomingRate;
         }

         if(backPressure.tryIntervalLock(this.windowSize)) {
            isUpdated = true;

            try {
               RateLimiter limiter = backPressure.rateLimiter;
               if(outgoingRate > 0.0D) {
                  double actualRatio = incomingRate / outgoingRate;
                  double limiterRate = limiter.getRate();
                  double newRate;
                  if(actualRatio >= this.highRatio) {
                     if(limiterRate <= outgoingRate) {
                        newRate = limiterRate + limiterRate * (double)this.factor / 100.0D;
                        if(newRate > 0.0D && newRate != 1.0D / 0.0) {
                           limiter.setRate(newRate);
                        }
                     }
                  } else {
                     newRate = incomingRate - incomingRate * (double)this.factor / 100.0D;
                     if(newRate > 0.0D && newRate < limiterRate) {
                        limiter.setRate(newRate);
                     }
                  }

                  if(logger.isTraceEnabled()) {
                     logger.trace("Back-pressure state for {}: incoming rate {}, outgoing rate {}, ratio {}, rate limiting {}", new Object[]{backPressure.getHost(), Double.valueOf(incomingRate), Double.valueOf(outgoingRate), Double.valueOf(actualRatio), Double.valueOf(limiter.getRate())});
                  }
               } else {
                  limiter.setRate(1.0D / 0.0);
               }

               backPressure.incomingRate.prune();
               backPressure.outgoingRate.prune();
            } finally {
               backPressure.releaseIntervalLock();
            }
         }

         if(backPressure.rateLimiter.getRate() <= minRateLimit) {
            minRateLimit = backPressure.rateLimiter.getRate();
            currentMin = backPressure.rateLimiter;
         }

         if(backPressure.rateLimiter.getRate() >= maxRateLimit) {
            maxRateLimit = backPressure.rateLimiter.getRate();
            currentMax = backPressure.rateLimiter;
         }
      }

      if(!states.isEmpty()) {
         RateBasedBackPressure.IntervalRateLimiter rateLimiter = (RateBasedBackPressure.IntervalRateLimiter)this.rateLimiters.get(states, (key) -> {
            return new RateBasedBackPressure.IntervalRateLimiter(this.timeSource);
         });
         if(isUpdated && rateLimiter.tryIntervalLock(this.windowSize)) {
            try {
               if(this.flow.equals(RateBasedBackPressure.Flow.FAST)) {
                  rateLimiter.limiter = currentMax;
               } else {
                  rateLimiter.limiter = currentMin;
               }

               tenSecsNoSpamLogger.info("{} currently applied for remote replicas: {}", new Object[]{rateLimiter.limiter, states});
            } finally {
               rateLimiter.releaseIntervalLock();
            }
         }

         long responseTimeInNanos = (long)((double)TimeUnit.NANOSECONDS.convert(1L, TimeUnit.SECONDS) / minIncomingRate);
         return this.doRateLimit(() -> {
            return Boolean.valueOf(rateLimiter.limiter.tryAcquire(1));
         }, (r, t) -> {
            TPC.bestTPCTimer().onTimeout(r, t.longValue(), TimeUnit.NANOSECONDS);
         }, rateLimiter.limiter.getRate(), Math.max(0L, TimeUnit.NANOSECONDS.convert(timeout, unit) - responseTimeInNanos)).thenAccept((u) -> {
         });
      } else {
         return CompletableFuture.completedFuture(null);
      }
   }

   public RateBasedBackPressureState newState(InetAddress host) {
      return new RateBasedBackPressureState(host, this.timeSource, this.windowSize);
   }

   @VisibleForTesting
   RateLimiter getRateLimiterForReplicaGroup(Set<RateBasedBackPressureState> states) {
      RateBasedBackPressure.IntervalRateLimiter rateLimiter = (RateBasedBackPressure.IntervalRateLimiter)this.rateLimiters.getIfPresent(states);
      return rateLimiter != null?rateLimiter.limiter:RateLimiter.create(1.0D / 0.0);
   }

   @VisibleForTesting
   CompletableFuture<Boolean> doRateLimit(Callable<Boolean> rateLimiter, BiConsumer<Runnable, Long> timer, double limitedRate, long timeoutInNanos) {
      try {
         if(((Boolean)rateLimiter.call()).booleanValue()) {
            return CompletableFuture.completedFuture(Boolean.valueOf(true));
         }
      } catch (Exception var9) {
         throw new IllegalStateException(var9);
      }

      CompletableFuture<Boolean> completion = new CompletableFuture();
      TPCRunnable backpressureTask = new TPCRunnable(() -> {
         completion.complete(Boolean.valueOf(false));
      }, ExecutorLocals.create(), TPCTaskType.NETWORK_BACKPRESSURE, TPC.getCoreId());
      backpressureTask.setPending();
      timer.accept(() -> {
         backpressureTask.unsetPending();
         backpressureTask.run();
      }, Long.valueOf(timeoutInNanos));
      return completion;
   }

   static {
      tenSecsNoSpamLogger = NoSpamLogger.getLogger(logger, 10L, TimeUnit.SECONDS);
      oneMinNoSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.MINUTES);
   }

   private static class IntervalRateLimiter extends IntervalLock {
      public volatile RateLimiter limiter = RateLimiter.create(1.0D / 0.0);

      IntervalRateLimiter(TimeSource timeSource) {
         super(timeSource);
      }
   }

   static enum Flow {
      FAST,
      SLOW;

      private Flow() {
      }
   }
}
