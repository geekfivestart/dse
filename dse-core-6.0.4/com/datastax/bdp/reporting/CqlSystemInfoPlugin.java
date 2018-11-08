package com.datastax.bdp.reporting;

import com.datastax.bdp.cassandra.metrics.PerformanceObjectsPlugin;
import com.datastax.bdp.plugin.DsePlugin;
import com.datastax.bdp.plugin.PerformanceObjectsController;
import com.datastax.bdp.plugin.ThreadPoolPlugin;
import com.datastax.bdp.plugin.bean.SnapshotInfoBean;
import com.datastax.bdp.reporting.snapshots.AbstractScheduledPlugin;
import com.datastax.bdp.server.system.ThreadPoolStatsProvider;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CacheService;

@Singleton
@DsePlugin(
   dependsOn = {PerformanceObjectsPlugin.class, ThreadPoolPlugin.class}
)
public class CqlSystemInfoPlugin extends AbstractScheduledPlugin<SnapshotInfoBean> {
   private List<PersistedSystemInfo> systemInfos;

   @Inject
   public CqlSystemInfoPlugin(PerformanceObjectsController.CqlSystemInfoBean bean, ThreadPoolPlugin threadPool) {
      super(threadPool, bean, true);
   }

   public void setupSchema() {
      int ttl = this.getTTL();
      this.systemInfos = ImmutableList.of(new KeyCacheInfo(this.nodeAddress, ttl, CacheService.instance.keyCache), new NetStatsInfo(this.nodeAddress, ttl, MessagingService.instance()), new ThreadPoolInfo(this.nodeAddress, ttl, new ThreadPoolStatsProvider.JMXThreadPoolProvider()), new ThreadPoolMessagesInfo(this.nodeAddress, ttl, MessagingService.instance()));
      Iterator var2 = this.systemInfos.iterator();

      while(var2.hasNext()) {
         PersistedSystemInfo info = (PersistedSystemInfo)var2.next();
         info.createTable();
      }

   }

   protected Runnable getTask() {
      return new Runnable() {
         public void run() {
            Iterator var1 = CqlSystemInfoPlugin.this.systemInfos.iterator();

            while(var1.hasNext()) {
               final PersistedSystemInfo info = (PersistedSystemInfo)var1.next();
               CqlSystemInfoPlugin.this.getThreadPool().submit(new Runnable() {
                  public void run() {
                     info.write(info);
                  }
               });
            }

         }
      };
   }

   protected int getInitialDelay() {
      return Boolean.getBoolean("dse.cql_system_info_no_initial_delay")?0:Math.max(1, (new Random()).nextInt(10));
   }
}
