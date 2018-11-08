package org.apache.cassandra.utils;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopKSampler<T> {
   private static final Logger logger = LoggerFactory.getLogger(TopKSampler.class);
   private volatile boolean enabled = false;
   @VisibleForTesting
   static final ThreadPoolExecutor samplerExecutor;
   private StreamSummary<T> summary;
   @VisibleForTesting
   HyperLogLogPlus hll;

   public TopKSampler() {
   }

   public synchronized void beginSampling(int capacity) {
      if(!this.enabled) {
         this.summary = new StreamSummary(capacity);
         this.hll = new HyperLogLogPlus(14);
         this.enabled = true;
      }

   }

   public synchronized TopKSampler.SamplerResult<T> finishSampling(int count) {
      List<Counter<T>> results = Collections.EMPTY_LIST;
      long cardinality = 0L;
      if(this.enabled) {
         this.enabled = false;
         results = this.summary.topK(count);
         cardinality = this.hll.cardinality();
      }

      return new TopKSampler.SamplerResult(results, cardinality);
   }

   public void addSample(T item) {
      this.addSample(item, (long)item.hashCode(), 1);
   }

   public void addSample(final T item, final long hash, final int value) {
      if(this.enabled) {
         samplerExecutor.execute(new Runnable() {
            public void run() {
               Object var1 = TopKSampler.this;
               synchronized(TopKSampler.this) {
                  if(TopKSampler.this.enabled) {
                     try {
                        TopKSampler.this.summary.offer(item, value);
                        TopKSampler.this.hll.offerHashed(hash);
                     } catch (Exception var4) {
                        TopKSampler.logger.trace("Failure to offer sample", var4);
                     }
                  }

               }
            }
         });
      }

   }

   static {
      samplerExecutor = new JMXEnabledThreadPoolExecutor(1, 1L, TimeUnit.SECONDS, new LinkedBlockingQueue(), new NamedThreadFactory("Sampler"), "internal");
   }

   public static class SamplerResult<S> implements Serializable {
      public final List<Counter<S>> topK;
      public final long cardinality;

      public SamplerResult(List<Counter<S>> topK, long cardinality) {
         this.topK = topK;
         this.cardinality = cardinality;
      }
   }
}
