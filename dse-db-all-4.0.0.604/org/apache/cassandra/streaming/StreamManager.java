package org.apache.cassandra.streaming;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.streaming.management.StreamEventJMXNotifier;
import org.apache.cassandra.streaming.management.StreamStateCompositeData;
import org.jctools.maps.NonBlockingHashMap;

public class StreamManager implements StreamManagerMBean {
   public static final StreamManager instance = new StreamManager();
   private final StreamEventJMXNotifier notifier = new StreamEventJMXNotifier();
   private final Map<UUID, StreamResultFuture> initiatedStreams = new NonBlockingHashMap();
   private final Map<UUID, StreamResultFuture> receivingStreams = new NonBlockingHashMap();

   public StreamManager() {
   }

   public static StreamManager.StreamRateLimiter getRateLimiter(InetAddress peer) {
      return new StreamManager.StreamRateLimiter(peer);
   }

   public Set<CompositeData> getCurrentStreams() {
      return Sets.newHashSet(Iterables.transform(Iterables.concat(this.initiatedStreams.values(), this.receivingStreams.values()), new Function<StreamResultFuture, CompositeData>() {
         public CompositeData apply(StreamResultFuture input) {
            return StreamStateCompositeData.toCompositeData(input.getCurrentState());
         }
      }));
   }

   public void register(final StreamResultFuture result) {
      result.addEventListener(this.notifier);
      result.addListener(new Runnable() {
         public void run() {
            StreamManager.this.initiatedStreams.remove(result.planId);
         }
      }, MoreExecutors.directExecutor());
      this.initiatedStreams.put(result.planId, result);
   }

   public void registerReceiving(final StreamResultFuture result) {
      result.addEventListener(this.notifier);
      result.addListener(new Runnable() {
         public void run() {
            StreamManager.this.receivingStreams.remove(result.planId);
         }
      }, MoreExecutors.directExecutor());
      this.receivingStreams.put(result.planId, result);
   }

   public StreamResultFuture getReceivingStream(UUID planId) {
      return (StreamResultFuture)this.receivingStreams.get(planId);
   }

   public void addNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback) {
      this.notifier.addNotificationListener(listener, filter, handback);
   }

   public void removeNotificationListener(NotificationListener listener) throws ListenerNotFoundException {
      this.notifier.removeNotificationListener(listener);
   }

   public void removeNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback) throws ListenerNotFoundException {
      this.notifier.removeNotificationListener(listener, filter, handback);
   }

   public MBeanNotificationInfo[] getNotificationInfo() {
      return this.notifier.getNotificationInfo();
   }

   public static class StreamRateLimiter {
      private static final double BYTES_PER_MEGABIT = 131072.0D;
      private static final RateLimiter limiter = RateLimiter.create(1.7976931348623157E308D);
      private static final RateLimiter interDCLimiter = RateLimiter.create(1.7976931348623157E308D);
      private final boolean isLocalDC;

      public StreamRateLimiter(InetAddress peer) {
         double throughput = (double)DatabaseDescriptor.getStreamThroughputOutboundMegabitsPerSec() * 131072.0D;
         this.mayUpdateThroughput(throughput, limiter);
         double interDCThroughput = (double)DatabaseDescriptor.getInterDCStreamThroughputOutboundMegabitsPerSec() * 131072.0D;
         this.mayUpdateThroughput(interDCThroughput, interDCLimiter);
         if(DatabaseDescriptor.getEndpointSnitch() != null && DatabaseDescriptor.getLocalDataCenter() != null) {
            this.isLocalDC = DatabaseDescriptor.getEndpointSnitch().isInLocalDatacenter(peer);
         } else {
            this.isLocalDC = true;
         }

      }

      private void mayUpdateThroughput(double limit, RateLimiter rateLimiter) {
         if(limit == 0.0D) {
            limit = 1.7976931348623157E308D;
         }

         if(rateLimiter.getRate() != limit) {
            rateLimiter.setRate(limit);
         }

      }

      public void acquire(int toTransfer) {
         limiter.acquire(toTransfer);
         if(!this.isLocalDC) {
            interDCLimiter.acquire(toTransfer);
         }

      }
   }
}
