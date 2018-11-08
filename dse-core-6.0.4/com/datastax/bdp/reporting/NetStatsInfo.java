package com.datastax.bdp.reporting;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.utils.ByteBufferUtil;

public class NetStatsInfo extends PersistedSystemInfo {
   public static final String NETSTATS_INSERT = String.format("INSERT INTO %s.%s (node_ip,read_repair_attempted,read_repaired_blocking,read_repaired_background,commands_pending,commands_completed,responses_pending,responses_completed)VALUES (?,?,?,?,?,?,?,?) USING TTL ?", new Object[]{"dse_perf", "net_stats"});
   private final MessagingServiceMBean messaging;

   public NetStatsInfo(InetAddress nodeAddress, int ttl, MessagingServiceMBean messaging) {
      super(nodeAddress, ttl);
      this.messaging = messaging;
   }

   protected String getTableName() {
      return "net_stats";
   }

   protected String getInsertCQL() {
      return NETSTATS_INSERT;
   }

   protected List<ByteBuffer> getVariables() {
      long largeMessageCompleted = this.sumLongs(this.messaging.getLargeMessageCompletedTasks().values());
      int largeMessagePending = this.sumInts(this.messaging.getLargeMessagePendingTasks().values());
      long smallMessageCompleted = this.sumLongs(this.messaging.getSmallMessageCompletedTasks().values());
      int smallMessagePending = this.sumInts(this.messaging.getSmallMessagePendingTasks().values());
      List<ByteBuffer> vars = new ArrayList();
      vars.add(this.nodeAddressBytes);
      vars.add(ByteBufferUtil.bytes(ReadRepairMetrics.attempted.getCount()));
      vars.add(ByteBufferUtil.bytes(ReadRepairMetrics.repairedBlocking.getCount()));
      vars.add(ByteBufferUtil.bytes(ReadRepairMetrics.repairedBackground.getCount()));
      vars.add(ByteBufferUtil.bytes(largeMessagePending));
      vars.add(ByteBufferUtil.bytes(largeMessageCompleted));
      vars.add(ByteBufferUtil.bytes(smallMessagePending));
      vars.add(ByteBufferUtil.bytes(smallMessageCompleted));
      vars.add(this.getTtlBytes());
      return vars;
   }

   private long sumLongs(Iterable<Long> values) {
      long total = 0L;

      long val;
      for(Iterator var4 = values.iterator(); var4.hasNext(); total += val) {
         val = ((Long)var4.next()).longValue();
      }

      return total;
   }

   private int sumInts(Iterable<Integer> values) {
      int total = 0;

      int val;
      for(Iterator var3 = values.iterator(); var3.hasNext(); total += val) {
         val = ((Integer)var3.next()).intValue();
      }

      return total;
   }
}
