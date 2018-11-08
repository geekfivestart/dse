package com.datastax.bdp.server.system;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import javax.management.MBeanServer;

public interface ThreadPoolStatsProvider {
   Map<ThreadPoolStats.Pool, ThreadPoolStats> getThreadPools();

   public static class JMXThreadPoolProvider implements ThreadPoolStatsProvider {
      private Map<ThreadPoolStats.Pool, ThreadPoolStats> pools;

      public JMXThreadPoolProvider() {
      }

      public Map<ThreadPoolStats.Pool, ThreadPoolStats> getThreadPools() {
         if(this.pools == null) {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            this.pools = new HashMap();
            ThreadPoolStats.Pool[] var2 = ThreadPoolStats.Pool.values();
            int var3 = var2.length;

            for(int var4 = 0; var4 < var3; ++var4) {
               ThreadPoolStats.Pool pool = var2[var4];
               this.pools.put(pool, new ThreadPoolStats(server, pool));
            }
         }

         return this.pools;
      }
   }
}
