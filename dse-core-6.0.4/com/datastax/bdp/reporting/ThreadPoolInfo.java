package com.datastax.bdp.reporting;

import com.datastax.bdp.server.system.ThreadPoolStats;
import com.datastax.bdp.server.system.ThreadPoolStatsProvider;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ThreadPoolInfo extends PersistedSystemInfo {
   public static final String THREAD_POOL_INSERT = String.format("INSERT INTO %s.%s (node_ip,pool_name,active,pending,completed,blocked,all_time_blocked)VALUES (?,?,?,?,?,?,?) USING TTL ? ;", new Object[]{"dse_perf", "thread_pool"});
   private final ThreadPoolStatsProvider poolProvider;

   public ThreadPoolInfo(InetAddress nodeAddress, int ttl, ThreadPoolStatsProvider threadPoolProvider) {
      super(nodeAddress, ttl);
      this.poolProvider = threadPoolProvider;
   }

   protected String getTableName() {
      return "thread_pool";
   }

   protected String getInsertCQL() {
      StringBuilder builder = new StringBuilder("BEGIN UNLOGGED BATCH ");
      Iterator var2 = this.poolProvider.getThreadPools().values().iterator();

      while(var2.hasNext()) {
         ThreadPoolStats pool = (ThreadPoolStats)var2.next();
         builder.append(THREAD_POOL_INSERT);
      }

      builder.append("APPLY BATCH");
      return builder.toString();
   }

   protected List<ByteBuffer> getVariables() {
      List<ByteBuffer> vars = new ArrayList();
      Iterator var2 = this.poolProvider.getThreadPools().values().iterator();

      while(var2.hasNext()) {
         ThreadPoolStats pool = (ThreadPoolStats)var2.next();
         vars.add(this.nodeAddressBytes);
         vars.add(ByteBufferUtil.bytes(pool.getName()));
         vars.add(ByteBufferUtil.bytes(pool.getActiveTasks()));
         vars.add(ByteBufferUtil.bytes(pool.getPendingTasks()));
         vars.add(ByteBufferUtil.bytes(pool.getCompletedTasks()));
         vars.add(ByteBufferUtil.bytes(pool.getCurrentlyBlockedTasks()));
         vars.add(ByteBufferUtil.bytes(pool.getTotalBlockedTasks()));
         vars.add(this.getTtlBytes());
      }

      return vars;
   }
}
