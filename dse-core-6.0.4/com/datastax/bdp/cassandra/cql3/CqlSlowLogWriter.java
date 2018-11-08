package com.datastax.bdp.cassandra.cql3;

import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.plugin.ThreadPoolPlugin;
import com.datastax.bdp.reporting.CqlWriter;
import com.datastax.bdp.util.Addresses;
import com.datastax.bdp.util.QueryProcessorUtil;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CqlSlowLogWriter {
   private static final Logger logger = LoggerFactory.getLogger(CqlSlowLogWriter.class);
   public static final String CQL_NODE_SLOW_LOG_INSERT = String.format("INSERT INTO %s.%s (node_ip,date,table_names,source_ip,username,start_time,duration,commands,parameters,tracing_session_id)VALUES (?,?,?,?,?,?,?,?,?,?) USING TTL ?", new Object[]{"dse_perf", "node_slow_log"});
   private static final boolean SLOW_QUERIES_FOREGROUND = Boolean.getBoolean("log_slow_queries_foreground");
   private final ByteBuffer nodeIp;
   private final ByteBuffer slowLogTTL;
   private final ByteBuffer emptyParamMap;
   private final ThreadPoolPlugin threadPool;
   private volatile CQLStatement insertStatement;

   public CqlSlowLogWriter(ThreadPoolPlugin threadPool) {
      this.threadPool = threadPool;
      this.nodeIp = ByteBufferUtil.bytes(Addresses.Internode.getBroadcastAddress());
      this.slowLogTTL = ByteBufferUtil.bytes(DseConfig.getCqlSlowLogTTL());
      this.emptyParamMap = MapSerializer.getInstance(UTF8Serializer.instance, UTF8Serializer.instance, UTF8Type.instance).serialize(Collections.emptyMap());
   }

   public synchronized void activate() {
      if(this.insertStatement == null) {
         this.insertStatement = prepareStatement(CQL_NODE_SLOW_LOG_INSERT, QueryState.forInternalCalls());
      }

   }

   public void recordSlowOperation(Set<Pair<String, String>> keyspaceTablePairs, InetAddress sourceIp, String username, UUID startTimeUUID, long duration, List<String> cqlStrings, UUID tracingSessionId) {
      Runnable task = () -> {
         logger.debug("Recording statements with duration of {} in slow log", Long.valueOf(duration));
         doInsert(this.insertStatement, this.getVariables(keyspaceTablePairs, sourceIp, username, startTimeUUID, duration, cqlStrings, tracingSessionId));
      };
      if(SLOW_QUERIES_FOREGROUND) {
         task.run();
      } else {
         this.threadPool.submit(task);
      }

   }

   private static CQLStatement prepareStatement(String cql, QueryState queryState) {
      return StatementUtils.prepareStatementBlocking(cql, queryState, "Error preparing cql slow log writer");
   }

   private List<ByteBuffer> getVariables(Set<Pair<String, String>> tables, InetAddress sourceIp, String username, UUID startTime, long duration, List<String> cqlStrings, UUID tracingSessionId) {
      List<ByteBuffer> vars = new ArrayList();
      vars.add(this.nodeIp);
      vars.add(TimestampType.instance.decompose(getUTCMidnight(UUIDGen.unixTimestamp(startTime))));
      vars.add(getTableSet(tables));
      vars.add(ByteBufferUtil.bytes(sourceIp));
      vars.add(ByteBufferUtil.bytes(username));
      vars.add(ByteBufferUtil.bytes(startTime));
      vars.add(ByteBufferUtil.bytes(duration));
      vars.add(getCommandList(cqlStrings));
      vars.add(this.emptyParamMap);
      vars.add(null == tracingSessionId?null:ByteBufferUtil.bytes(tracingSessionId));
      vars.add(this.slowLogTTL);
      return vars;
   }

   public static Date getUTCMidnight(long instant) {
      return (new DateTime(instant, DateTimeZone.UTC)).toDateMidnight().toDate();
   }

   private static ByteBuffer getCommandList(List<String> strings) {
      return ListType.getInstance(UTF8Type.instance, true).decompose(strings);
   }

   private static ByteBuffer getTableSet(Set<Pair<String, String>> pairs) {
      return SetType.getInstance(UTF8Type.instance, true).decompose(ImmutableSet.copyOf(Iterables.filter(Iterables.transform(pairs, new Function<Pair<String, String>, String>() {
         public String apply(Pair<String, String> input) {
            return input.right == null?null:((String)input.left).concat(".").concat((String)input.right);
         }
      }), Predicates.notNull())));
   }

   private static void doInsert(CQLStatement statement, List<ByteBuffer> variables) {
      try {
         QueryProcessorUtil.processPreparedBlocking(statement, ConsistencyLevel.ONE, variables);
      } catch (Exception var3) {
         CqlWriter.handleWriteException("node_slow_log", var3);
      }

   }
}
