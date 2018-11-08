package com.datastax.bdp.plugin;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Future;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public interface SparkPluginBean extends SparkPluginMXBean {
   Future<ResultMessage> executeQueryOnNode(InetAddress var1, String var2, QueryState var3, QueryOptions var4, Map<String, ByteBuffer> var5);

   Future<ResultMessage> executeQueryOnMasterNode(String var1, QueryState var2, QueryOptions var3, Map<String, ByteBuffer> var4);
}
