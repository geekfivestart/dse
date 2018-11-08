package com.datastax.bdp.cassandra.metrics;

import com.datastax.bdp.cassandra.cql3.StatementUtils;
import com.datastax.bdp.plugin.bean.UserLatencyTrackingBean;
import com.datastax.bdp.util.Addresses;
import com.datastax.bdp.util.QueryProcessorUtil;
import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.BatchStatement.Type;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.serializers.DoubleSerializer;
import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class UserLatencyMetricsWriter implements Runnable {
   private static final Logger logger = LoggerFactory.getLogger(UserLatencyMetricsWriter.class);
   private final CQLStatement snapshotUserObjectReadInsertStatement;
   private final CQLStatement snapshotUserObjectReadDeleteStatement;
   private final CQLStatement snapshotObjectUserReadInsertStatement;
   private final CQLStatement snapshotObjectUserReadDeleteStatement;
   private final CQLStatement snapshotUserReadInsertStatement;
   private final CQLStatement snapshotUserReadDeleteStatement;
   private final CQLStatement snapshotUserObjectWriteInsertStatement;
   private final CQLStatement snapshotUserObjectWriteDeleteStatement;
   private final CQLStatement snapshotObjectUserWriteInsertStatement;
   private final CQLStatement snapshotObjectUserWriteDeleteStatement;
   private final CQLStatement snapshotUserWriteInsertStatement;
   private final CQLStatement snapshotUserWriteDeleteStatement;
   private final CQLStatement userObjectIOStatement;
   private final CQLStatement objectUserIOStatement;
   private final CQLStatement userIOStatement;
   private final CQLStatement[] userObjectDeletes;
   private final CQLStatement[] userAggregateDeletes;
   private final int snapshotTTL = Integer.getInteger("dse.user_latency_metrics_snapshot_ttl", 600).intValue();
   private final int longTermTTL = Integer.getInteger("dse.user_latency_metrics_lifetime_ttl", 600).intValue();
   private final AtomicInteger topStatsLimit = new AtomicInteger();
   private final ByteBuffer nodeAddressBytes;
   private final MapSerializer quantileSerializer;
   private final UserMetrics userMetrics;
   private int userObjectRowsWritten;
   private int userRowsWritten;

   @Inject
   public UserLatencyMetricsWriter(UserLatencyTrackingBean trackingBean, UserMetrics userMetrics) {
      this.topStatsLimit.set(trackingBean.getTopStatsLimit());
      QueryState queryState = QueryState.forInternalCalls();
      this.snapshotUserObjectReadInsertStatement = this.prepareStatement(UserObjectCqlConstants.USER_OBJECT_READ_IO_INSERT, queryState);
      this.snapshotUserObjectReadDeleteStatement = this.prepareStatement(UserObjectCqlConstants.USER_OBJECT_READ_IO_DELETE, queryState);
      this.snapshotObjectUserReadInsertStatement = this.prepareStatement(UserObjectCqlConstants.OBJECT_USER_READ_IO_INSERT, queryState);
      this.snapshotObjectUserReadDeleteStatement = this.prepareStatement(UserObjectCqlConstants.OBJECT_USER_READ_IO_DELETE, queryState);
      this.snapshotUserReadInsertStatement = this.prepareStatement(UserObjectCqlConstants.USER_READ_IO_INSERT, queryState);
      this.snapshotUserReadDeleteStatement = this.prepareStatement(UserObjectCqlConstants.USER_READ_IO_DELETE, queryState);
      this.snapshotUserObjectWriteInsertStatement = this.prepareStatement(UserObjectCqlConstants.USER_OBJECT_WRITE_IO_INSERT, queryState);
      this.snapshotUserObjectWriteDeleteStatement = this.prepareStatement(UserObjectCqlConstants.USER_OBJECT_WRITE_IO_DELETE, queryState);
      this.snapshotObjectUserWriteInsertStatement = this.prepareStatement(UserObjectCqlConstants.OBJECT_USER_WRITE_IO_INSERT, queryState);
      this.snapshotObjectUserWriteDeleteStatement = this.prepareStatement(UserObjectCqlConstants.OBJECT_USER_WRITE_IO_DELETE, queryState);
      this.snapshotUserWriteInsertStatement = this.prepareStatement(UserObjectCqlConstants.USER_WRITE_IO_INSERT, queryState);
      this.snapshotUserWriteDeleteStatement = this.prepareStatement(UserObjectCqlConstants.USER_WRITE_IO_DELETE, queryState);
      this.userObjectIOStatement = this.prepareStatement(UserObjectCqlConstants.USER_OBJECT_IO_INSERT, queryState);
      this.objectUserIOStatement = this.prepareStatement(UserObjectCqlConstants.OBJECT_USER_IO_INSERT, queryState);
      this.userIOStatement = this.prepareStatement(UserObjectCqlConstants.USER_IO_INSERT, queryState);
      this.userObjectDeletes = new CQLStatement[]{this.snapshotObjectUserWriteDeleteStatement, this.snapshotObjectUserReadDeleteStatement, this.snapshotUserObjectWriteDeleteStatement, this.snapshotUserObjectReadDeleteStatement};
      this.userAggregateDeletes = new CQLStatement[]{this.snapshotUserReadDeleteStatement, this.snapshotUserWriteDeleteStatement};
      this.quantileSerializer = MapSerializer.getInstance(DoubleSerializer.instance, DoubleSerializer.instance, DoubleType.instance);
      this.nodeAddressBytes = ByteBufferUtil.bytes(Addresses.Internode.getBroadcastAddress());
      logger.debug("Initialized user latency metrics writer ({}/{})", Integer.valueOf(this.snapshotTTL), Integer.valueOf(this.longTermTTL));
      this.userMetrics = userMetrics;
   }

   public void unhook(UserLatencyTrackingBean latencyBean) {
      this.userMetrics.unhook(latencyBean);
   }

   public UserMetrics getUserMetrics() {
      return this.userMetrics;
   }

   private CQLStatement prepareStatement(String cql, QueryState queryState) {
      return StatementUtils.prepareStatementBlocking(cql, queryState, "Error preparing io latency tracking query");
   }

   public synchronized void run() {
      try {
         if(Boolean.getBoolean("dse.noop_user_metrics")) {
            return;
         }

         logger.debug("Processing user/object io metrics");
         Iterable<RawUserObjectLatency> allMetrics = this.userMetrics.getAllMetrics();
         logger.debug("Retrieved user metrics records");
         int limit = this.topStatsLimit.get();
         UserLatencyMetricsProcessor processor = new UserLatencyMetricsProcessor(allMetrics, limit);
         logger.debug("Analysed user metrics");
         this.userObjectRowsWritten = processor.getTopUserObjectsByRead().size();
         this.userRowsWritten = processor.getTopUsersByRead().size();
         this.writeObjectUserUpdates(allMetrics, processor.getTopUserObjectsByRead(), processor.getTopUserObjectsByWrite());
         logger.debug("Written object user updates");
         this.writeUserAggregateUpdates(processor.getAllUserActivity(), processor.getTopUsersByRead(), processor.getTopUsersByWrite());
         logger.debug("Written aggregated user updates");
         this.issueSnapshotDeletes(this.userObjectDeletes, this.userObjectRowsWritten, limit);
         this.issueSnapshotDeletes(this.userAggregateDeletes, this.userRowsWritten, limit);
      } catch (RuntimeException var4) {
         logger.debug("Caught exception during periodic processing of user latency metrics", var4);
      }

   }

   private void issueSnapshotDeletes(CQLStatement[] deleteStatements, int rowsWritten, int limit) {
      if(rowsWritten < limit) {
         try {
            List<ModificationStatement> stmts = new ArrayList();
            List<List<ByteBuffer>> values = new ArrayList();

            for(int i = rowsWritten; i < limit; ++i) {
               ByteBuffer indexBytes = ByteBufferUtil.bytes(i);
               CQLStatement[] var8 = deleteStatements;
               int var9 = deleteStatements.length;

               for(int var10 = 0; var10 < var9; ++var10) {
                  CQLStatement stmt = var8[var10];
                  stmts.add((ModificationStatement)stmt);
                  values.add(Lists.newArrayList(new ByteBuffer[]{this.nodeAddressBytes, indexBytes}));
               }
            }

            BatchStatement batch = new BatchStatement(-1, Type.UNLOGGED, stmts, Attributes.none());
            QueryProcessorUtil.processBatchBlocking(batch, ConsistencyLevel.ONE, values);
         } catch (Exception var12) {
            logger.debug("Error trimming stale rows from user latency metrics snapshot table. Values will be left to expire", var12);
         }

      }
   }

   private void writeUserAggregateUpdates(Iterable<AggregateUserLatency> allMetrics, Queue<AggregateUserLatency> topReadMetrics, Queue<AggregateUserLatency> topWriteMetrics) {
      ByteBuffer timestamp = ByteBufferUtil.bytes(System.currentTimeMillis());
      int var7 = 0;

      List variables;
      AggregateUserLatency a;
      while((a = (AggregateUserLatency)topReadMetrics.poll()) != null) {
         variables = this.getUserAggregateVariables(a, true, (ByteBuffer)null, var7++);
         this.doInsert(this.snapshotUserReadInsertStatement, variables);
      }

      var7 = 0;

      while((a = (AggregateUserLatency)topWriteMetrics.poll()) != null) {
         variables = this.getUserAggregateVariables(a, true, (ByteBuffer)null, var7++);
         this.doInsert(this.snapshotUserWriteInsertStatement, variables);
      }

      Iterator var8 = allMetrics.iterator();

      while(var8.hasNext()) {
         AggregateUserLatency metric = (AggregateUserLatency)var8.next();
         variables = this.getUserAggregateVariables(metric, false, timestamp, -1);
         this.doInsert(this.userIOStatement, variables);
      }

   }

   private void writeObjectUserUpdates(Iterable<RawUserObjectLatency> allMetrics, Queue<RawUserObjectLatency> topReadMetrics, Queue<RawUserObjectLatency> topWriteMetrics) {
      ByteBuffer timestamp = ByteBufferUtil.bytes(System.currentTimeMillis());
      int var7 = 0;

      List variables;
      RawUserObjectLatency m;
      while((m = (RawUserObjectLatency)topReadMetrics.poll()) != null) {
         variables = this.getUserObjectVariables(m, true, (ByteBuffer)null, var7++);
         this.doInsert(this.snapshotUserObjectReadInsertStatement, variables);
         this.doInsert(this.snapshotObjectUserReadInsertStatement, variables);
      }

      var7 = 0;

      while((m = (RawUserObjectLatency)topWriteMetrics.poll()) != null) {
         variables = this.getUserObjectVariables(m, true, (ByteBuffer)null, var7++);
         this.doInsert(this.snapshotUserObjectWriteInsertStatement, variables);
         this.doInsert(this.snapshotObjectUserWriteInsertStatement, variables);
      }

      Iterator var8 = allMetrics.iterator();

      while(var8.hasNext()) {
         RawUserObjectLatency metric = (RawUserObjectLatency)var8.next();
         variables = this.getUserObjectVariables(metric, false, timestamp, -1);
         this.doInsert(this.userObjectIOStatement, variables);
         this.doInsert(this.objectUserIOStatement, variables);
      }

   }

   private void doInsert(CQLStatement statement, List<ByteBuffer> variables) {
      try {
         QueryProcessorUtil.processPreparedBlocking(statement, ConsistencyLevel.ONE, variables);
      } catch (Exception var4) {
         logger.debug("Error writing io latency stats to perf subsystem", var4);
      }

   }

   private List<ByteBuffer> getUserAggregateVariables(AggregateUserLatency raw, boolean isSnapshot, ByteBuffer timestamp, int latencyIndex) {
      logger.debug("Converting user aggregate metrics to bind vars: {}", raw.toString());
      List<ByteBuffer> vars = new ArrayList();
      vars.add(this.nodeAddressBytes);
      vars.add(ByteBufferUtil.bytes(raw.connectionInfo.clientAddress));
      vars.add(ByteBufferUtil.bytes(raw.connectionInfo.connectionId));
      vars.add(ByteBufferUtil.bytes(raw.connectionInfo.userId));
      vars.add(ByteBufferUtil.bytes(this.round(raw.getMeanReadLatency())));
      vars.add(ByteBufferUtil.bytes(raw.getReadOps()));
      vars.add(ByteBufferUtil.bytes(this.round(raw.getMeanWriteLatency())));
      vars.add(ByteBufferUtil.bytes(raw.getWriteOps()));
      if(isSnapshot) {
         vars.add(ByteBufferUtil.bytes(latencyIndex));
         vars.add(ByteBufferUtil.bytes(this.snapshotTTL));
      } else {
         vars.add(timestamp);
         vars.add(ByteBufferUtil.bytes(this.longTermTTL));
      }

      return vars;
   }

   private List<ByteBuffer> getUserObjectVariables(RawUserObjectLatency raw, boolean isSnapshot, ByteBuffer timestamp, int latencyIndex) {
      logger.debug("Converting user/object metrics to bind vars: {}", raw);
      List<ByteBuffer> vars = new ArrayList();
      vars.add(ByteBufferUtil.bytes(raw.latency.keyspace));
      vars.add(ByteBufferUtil.bytes(raw.latency.columnFamily));
      vars.add(this.nodeAddressBytes);
      vars.add(ByteBufferUtil.bytes(raw.connectionInfo.clientAddress));
      vars.add(ByteBufferUtil.bytes(raw.connectionInfo.connectionId));
      vars.add(ByteBufferUtil.bytes(raw.connectionInfo.userId));
      vars.add(ByteBufferUtil.bytes(this.round(raw.latency.getMeanReadLatency())));
      vars.add(ByteBufferUtil.bytes(raw.latency.totalReads));
      vars.add(ByteBufferUtil.bytes(this.round(raw.latency.getMeanWriteLatency())));
      vars.add(ByteBufferUtil.bytes(raw.latency.totalWrites));
      vars.add(this.quantileSerializer.serialize(this.round(raw.latency.readQuantiles)));
      vars.add(this.quantileSerializer.serialize(this.round(raw.latency.writeQuantiles)));
      if(isSnapshot) {
         vars.add(ByteBufferUtil.bytes(latencyIndex));
         vars.add(ByteBufferUtil.bytes(this.snapshotTTL));
      } else {
         vars.add(timestamp);
         vars.add(ByteBufferUtil.bytes(this.longTermTTL));
      }

      return vars;
   }

   private double round(double value) {
      return (double)Math.round(value) * 0.001D;
   }

   public synchronized void setTopStatsLimit(int topStatsLimit) {
      this.issueSnapshotDeletes(this.userObjectDeletes, this.userObjectRowsWritten, topStatsLimit);
      this.issueSnapshotDeletes(this.userAggregateDeletes, this.userRowsWritten, topStatsLimit);
      this.topStatsLimit.set(topStatsLimit);
   }

   private SortedMap<Double, Double> round(SortedMap<Double, Double> value) {
      SortedMap<Double, Double> rounded = new TreeMap();
      Iterator var3 = value.entrySet().iterator();

      while(var3.hasNext()) {
         Entry<Double, Double> e = (Entry)var3.next();
         rounded.put(e.getKey(), Double.valueOf(this.round(((Double)e.getValue()).doubleValue())));
      }

      return rounded;
   }
}
