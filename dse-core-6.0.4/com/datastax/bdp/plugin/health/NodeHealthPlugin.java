package com.datastax.bdp.plugin.health;

import com.datastax.bdp.plugin.AbstractPlugin;
import com.datastax.bdp.plugin.DsePlugin;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@DsePlugin(
   dependsOn = {}
)
public class NodeHealthPlugin extends AbstractPlugin {
   private final int refreshPeriodMillis;
   private final NodeHealthPluginUpdater nodeHealthPluginTask;
   private ScheduledExecutorService nodeHealthPluginScheduler;
   private static final Logger logger = LoggerFactory.getLogger(NodeHealthPlugin.class);

   @Inject
   public NodeHealthPlugin(NodeHealthPluginUpdater nodeHealthPluginUpdater) {
      this.nodeHealthPluginTask = nodeHealthPluginUpdater;
      this.refreshPeriodMillis = this.nodeHealthPluginTask.getRefreshPeriodMillis();
   }

   public void onActivate() {
      this.startNodeHealthPlugin();
   }

   public boolean isEnabled() {
      return true;
   }

   private void startNodeHealthPlugin() {
      if(this.nodeHealthPluginScheduler == null) {
         logger.debug("Starting NodeHealthPlugin");
         String threadNameFormat = "NodeHealthPlugin-Scheduler-thread-%d";
         this.nodeHealthPluginScheduler = Executors.newSingleThreadScheduledExecutor((new ThreadFactoryBuilder()).setDaemon(true).setNameFormat(threadNameFormat).build());
         this.nodeHealthPluginScheduler.scheduleAtFixedRate(this.nodeHealthPluginTask, (long)this.refreshPeriodMillis, (long)this.refreshPeriodMillis, TimeUnit.MILLISECONDS);
      }

   }
}
