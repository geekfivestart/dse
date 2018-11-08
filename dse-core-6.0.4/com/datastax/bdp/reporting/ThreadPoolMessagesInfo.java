package com.datastax.bdp.reporting;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.net.DroppedMessages.Group;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ThreadPoolMessagesInfo extends PersistedSystemInfo {
   public static final String THREAD_POOL_MSGS_INSERT = String.format("INSERT INTO %s.%s (node_ip,message_type,dropped_count)VALUES (?,?,?) USING TTL ? ;", new Object[]{"dse_perf", "thread_pool_messages"});
   private final MessagingServiceMBean messaging;

   public ThreadPoolMessagesInfo(InetAddress nodeAddress, int ttl, MessagingServiceMBean messaging) {
      super(nodeAddress, ttl);
      this.messaging = messaging;
   }

   protected String getTableName() {
      return "thread_pool_messages";
   }

   protected String getInsertCQL() {
      StringBuilder builder = new StringBuilder("BEGIN UNLOGGED BATCH ");
      Group[] var2 = Group.values();
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         Group var10000 = var2[var4];
         builder.append(THREAD_POOL_MSGS_INSERT);
      }

      builder.append("APPLY BATCH");
      return builder.toString();
   }

   protected List<ByteBuffer> getVariables() {
      List<ByteBuffer> vars = new ArrayList();
      Map<String, Integer> droppedMessages = this.messaging.getDroppedMessages();
      Group[] var3 = Group.values();
      int var4 = var3.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         Group group = var3[var5];
         vars.add(this.nodeAddressBytes);
         vars.add(ByteBufferUtil.bytes(group.toString()));
         Integer count = (Integer)droppedMessages.get(group.toString());
         vars.add(count == null?ByteBufferUtil.bytes(0):ByteBufferUtil.bytes(count.intValue()));
         vars.add(this.getTtlBytes());
      }

      return vars;
   }
}
