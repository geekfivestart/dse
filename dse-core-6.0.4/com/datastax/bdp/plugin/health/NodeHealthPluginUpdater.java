package com.datastax.bdp.plugin.health;

import com.datastax.bdp.concurrent.metrics.SlidingTimeRate;
import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.gms.DseState;
import com.datastax.bdp.server.system.MessagingInfoProvider;
import com.datastax.bdp.system.SystemTimeSource;
import com.datastax.bdp.system.TimeSource;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Singleton;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class NodeHealthPluginUpdater implements Runnable {
   private static final Logger logger = LoggerFactory.getLogger(NodeHealthPluginUpdater.class);
   public static final double MAX_UPTIME_SCORE = 0.95D;
   private static final String MUTATION = "MUTATION";
   private static final int WINDOW_PRECISION_SECONDS = 30;
   private volatile int lastDropped;
   private final int refreshPeriodMillis;
   private final SlidingTimeRate windowedDropRate;
   private final double smoothingConstant;
   private final MessagingInfoProvider messaging;

   public NodeHealthPluginUpdater() {
      this(new SystemTimeSource(), DseConfig.getNodeHealthRefreshRate(), DseConfig.getDroppedMutationWindow(), (long)DseConfig.getUptimeRampUpPeriod());
   }

   public NodeHealthPluginUpdater(long uptimeRampUpPeriodSeconds) {
      this(new SystemTimeSource(), DseConfig.getNodeHealthRefreshRate(), DseConfig.getDroppedMutationWindow(), uptimeRampUpPeriodSeconds);
   }

   public NodeHealthPluginUpdater(TimeSource timeSource, int refreshPeriodMillis, int droppedMutationWindowMinutes, long uptimeRampUpPeriodSeconds) {
      this.lastDropped = 0;
      this.messaging = new MessagingInfoProvider.JmxMessagingInfoProvider();
      int windowSeconds = (int)TimeUnit.SECONDS.convert((long)droppedMutationWindowMinutes, TimeUnit.MINUTES);
      this.windowedDropRate = new SlidingTimeRate(timeSource, windowSeconds, 30, TimeUnit.SECONDS);
      this.refreshPeriodMillis = refreshPeriodMillis;
      this.smoothingConstant = 0.95D / ((double)uptimeRampUpPeriodSeconds - 0.95D * (double)uptimeRampUpPeriodSeconds);
   }

   public synchronized void run() {
      double uptime = this.getUptimeScore(ManagementFactory.getRuntimeMXBean().getUptime());
      int droppedMessages = ((Integer)this.messaging.getDroppedMessages().get("MUTATION")).intValue();
      if(logger.isDebugEnabled()) {
         logger.debug("Received {} dropped messages from MessagingService.", Integer.valueOf(droppedMessages));
      }

      this.updateDroppedMessages(droppedMessages);
      double dropRate = this.getDroppedMessagesRate();
      double nodeHealthFactor = NodeHealthFunction.apply(uptime, dropRate);
      if(logger.isDebugEnabled()) {
         logger.debug("Calculated node health of {} from uptime score of {} and dropped message rate of {}/second.", new Object[]{Double.valueOf(nodeHealthFactor), Double.valueOf(uptime), Double.valueOf(dropRate)});
      }

      DseState.instance.setNodeHealthAsync(nodeHealthFactor);
   }

   @VisibleForTesting
   public double getUptimeScore(long uptimeMillis) {
      double uptimeSeconds = (double)(uptimeMillis / 1000L);
      return uptimeSeconds * this.smoothingConstant / (1.0D + uptimeSeconds * this.smoothingConstant);
   }

   @VisibleForTesting
   public void updateDroppedMessages(int droppedMessages) {
      this.windowedDropRate.update(Math.max(0, droppedMessages - this.lastDropped));
      if(droppedMessages > 0) {
         this.lastDropped = droppedMessages;
      }

      this.windowedDropRate.prune();
   }

   @VisibleForTesting
   public double getDroppedMessagesRate() {
      return this.windowedDropRate.get(TimeUnit.SECONDS);
   }

   public int getRefreshPeriodMillis() {
      return this.refreshPeriodMillis;
   }
}
