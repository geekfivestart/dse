package com.datastax.bdp.cassandra.metrics;

import com.datastax.bdp.cassandra.cql3.StatementUtils;
import com.datastax.bdp.util.Addresses;
import com.datastax.bdp.util.QueryProcessorUtil;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.MemoryOnlyStrategy;
import org.apache.cassandra.serializers.BooleanSerializer;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeMetricsWriter implements Runnable {
   private static final Logger logger = LoggerFactory.getLogger(NodeMetricsWriter.class);
   private static final Function<NodeMetricsWriter.TableLatencyMetadata, RawObjectLatency> TRANSFORM = new Function<NodeMetricsWriter.TableLatencyMetadata, RawObjectLatency>() {
      public RawObjectLatency apply(NodeMetricsWriter.TableLatencyMetadata input) {
         return new RawObjectLatency(input.ks, input.table, input.recent.getValue(LatencyValues.EventType.READ), input.recent.getCount(LatencyValues.EventType.READ), input.recent.getValue(LatencyValues.EventType.WRITE), input.recent.getCount(LatencyValues.EventType.WRITE), input.recent.getQuantiles(LatencyValues.EventType.READ), input.recent.getQuantiles(LatencyValues.EventType.WRITE));
      }
   };
   private CQLStatement snapshotObjectReadInsertStatement;
   private CQLStatement snapshotObjectWriteInsertStatement;
   private CQLStatement objectIOStatement;
   private int snapshotTTL = Integer.getInteger("dse.object_latency_metrics_snapshot_ttl", 600).intValue();
   private int lifetimeTTL = Integer.getInteger("dse.object_latency_metrics_lifetime_ttl", 600).intValue();
   private ByteBuffer nodeAddressBytes = ByteBufferUtil.bytes(Addresses.Internode.getBroadcastAddress());
   private Map<Pair<String, String>, NodeMetricsWriter.TableLatencyMetadata> tableLatencyValues = new HashMap();

   public NodeMetricsWriter() {
      QueryState queryState = QueryState.forInternalCalls();
      this.snapshotObjectReadInsertStatement = this.prepareStatement(NodeMetricsCqlConstants.OBJECT_READ_IO_INSERT, queryState);
      this.snapshotObjectWriteInsertStatement = this.prepareStatement(NodeMetricsCqlConstants.OBJECT_WRITE_IO_INSERT, queryState);
      this.objectIOStatement = this.prepareStatement(NodeMetricsCqlConstants.OBJECT_IO_INSERT, queryState);
      logger.debug("Initialized object latency metrics writer ({}/{})", Integer.valueOf(this.snapshotTTL), Integer.valueOf(this.lifetimeTTL));
   }

   private static boolean isMemoryOnly(ColumnFamilyStore cfs) {
      List<List<AbstractCompactionStrategy>> strategies = cfs.getCompactionStrategyManager().getStrategies();
      return strategies.size() > 0 && ((List)strategies.get(0)).size() > 0 && ((List)strategies.get(0)).get(0) instanceof MemoryOnlyStrategy;
   }

   public void run() {
      try {
         logger.debug("Processing node/object io metrics");
         ByteBuffer timestamp = ByteBufferUtil.bytes(System.currentTimeMillis());
         Iterator var3 = Keyspace.all().iterator();

         while(true) {
            Keyspace ks;
            do {
               if(!var3.hasNext()) {
                  List<RawObjectLatency> sortableMetrics = Lists.newArrayList(Iterables.transform(this.tableLatencyValues.values(), TRANSFORM));
                  this.writeSnapshotData(this.snapshotObjectReadInsertStatement, sortableMetrics, RawObjectLatency.READ_LATENCY_COMPARATOR);
                  this.writeSnapshotData(this.snapshotObjectWriteInsertStatement, sortableMetrics, RawObjectLatency.WRITE_LATENCY_COMPARATOR);
                  return;
               }

               ks = (Keyspace)var3.next();
            } while(PerformanceObjectsPlugin.isUntracked(ks.getName()));

            logger.debug("Processing keyspace {}", ks.getName());
            Iterator var5 = ks.getColumnFamilyStores().iterator();

            while(var5.hasNext()) {
               ColumnFamilyStore cfs = (ColumnFamilyStore)var5.next();
               Pair<String, String> tableId = Pair.create(ks.getName(), cfs.name);
               boolean memoryOnly = isMemoryOnly(cfs);
               NodeMetricsWriter.TableLatencyMetadata latencyValues = (NodeMetricsWriter.TableLatencyMetadata)this.tableLatencyValues.get(tableId);
               if(latencyValues == null) {
                  latencyValues = new NodeMetricsWriter.TableLatencyMetadata((String)tableId.left, (String)tableId.right, memoryOnly);
                  this.tableLatencyValues.put(tableId, latencyValues);
               }

               if(this.wasActive(cfs, latencyValues.lifetime)) {
                  long totalReadLatency = cfs.metric.readLatency.totalLatency.getCount();
                  long totalReadCount = cfs.metric.readLatency.latency.getCount();
                  long totalWriteLatency = cfs.metric.writeLatency.totalLatency.getCount();
                  long totalWriteCount = cfs.metric.writeLatency.latency.getCount();
                  List<ByteBuffer> variables = this.getObjectVariables(ks.getName(), cfs.name, totalReadLatency, totalReadCount, totalWriteLatency, totalWriteCount, memoryOnly, timestamp, -1, false);
                  this.doInsert(this.objectIOStatement, variables);
                  latencyValues.updateLifetime(totalReadLatency, totalReadCount, LatencyValues.EventType.READ);
                  latencyValues.updateLifetime(totalWriteLatency, totalWriteCount, LatencyValues.EventType.WRITE);
               }
            }
         }
      } catch (Exception var18) {
         logger.debug("Caught exception during periodic processing of table latency metrics", var18);
      }
   }

   private void writeSnapshotData(CQLStatement statement, List<RawObjectLatency> metrics, Comparator<RawObjectLatency> comp) {
      Collections.sort(metrics, comp);
      int latencyIndex = 0;
      Iterator var5 = metrics.iterator();

      while(var5.hasNext()) {
         RawObjectLatency raw = (RawObjectLatency)var5.next();
         Pair<String, String> tableId = Pair.create(raw.keyspace, raw.columnFamily);
         NodeMetricsWriter.TableLatencyMetadata metadata = (NodeMetricsWriter.TableLatencyMetadata)this.tableLatencyValues.get(tableId);
         List<ByteBuffer> variables = this.getObjectVariables(metadata.ks, metadata.table, metadata.recent.getValue(LatencyValues.EventType.READ), metadata.recent.getCount(LatencyValues.EventType.READ), metadata.recent.getValue(LatencyValues.EventType.WRITE), metadata.recent.getCount(LatencyValues.EventType.WRITE), metadata.memoryOnly, (ByteBuffer)null, latencyIndex++, true);
         this.doInsert(statement, variables);
      }

   }

   private boolean wasActive(ColumnFamilyStore cfs, LatencyValues lastSeen) {
      return cfs.metric.readLatency.latency.getCount() != lastSeen.getCount(LatencyValues.EventType.READ) || cfs.metric.writeLatency.latency.getCount() != lastSeen.getCount(LatencyValues.EventType.WRITE);
   }

   private CQLStatement prepareStatement(String cql, QueryState queryState) {
      return StatementUtils.prepareStatementBlocking(cql, queryState, "Error preparing io latency tracking query");
   }

   private void doInsert(CQLStatement statement, List<ByteBuffer> variables) {
      try {
         QueryProcessorUtil.processPreparedBlocking(statement, ConsistencyLevel.ONE, variables);
      } catch (Exception var4) {
         logger.debug("Error writing io latency stats to perf subsystem", var4);
      }

   }

   private List<ByteBuffer> getObjectVariables(String ksName, String cfName, long totalReadLatency, long readCount, long totalWriteLatency, long writeCount, boolean memoryOnly, ByteBuffer timestamp, int latencyIndex, boolean isSnapshot) {
      logger.debug("Converting latency metrics to bind vars: {}.{}", ksName, cfName);
      List<ByteBuffer> vars = new ArrayList();
      vars.add(ByteBufferUtil.bytes(Addresses.Internode.getBroadcastAddress()));
      vars.add(ByteBufferUtil.bytes(ksName));
      vars.add(ByteBufferUtil.bytes(cfName));
      vars.add(ByteBufferUtil.bytes(this.getMeanValue(totalReadLatency, readCount)));
      vars.add(ByteBufferUtil.bytes(readCount));
      vars.add(ByteBufferUtil.bytes(this.getMeanValue(totalWriteLatency, writeCount)));
      vars.add(ByteBufferUtil.bytes(writeCount));
      vars.add(BooleanSerializer.instance.serialize(Boolean.valueOf(memoryOnly)));
      if(isSnapshot) {
         vars.add(ByteBufferUtil.bytes(latencyIndex));
         vars.add(ByteBufferUtil.bytes(this.snapshotTTL));
      } else {
         vars.add(timestamp);
         vars.add(ByteBufferUtil.bytes(this.lifetimeTTL));
      }

      return vars;
   }

   private double getMeanValue(long total, long count) {
      return count == 0L?0.0D:(double)Math.round((double)total / (double)count * 1000.0D) / 1000.0D;
   }

   private class TableLatencyMetadata {
      final String ks;
      final String table;
      final boolean memoryOnly;
      final LatencyValues lifetime;
      final LatencyValues recent;

      TableLatencyMetadata(String ks, String table, boolean memoryOnly) {
         this.ks = ks;
         this.table = table;
         this.memoryOnly = memoryOnly;
         this.lifetime = new LatencyValues();
         this.recent = new LatencyValues();
      }

      public void updateLifetime(long value, long count, LatencyValues.EventType type) {
         long newRecentCount = count - this.lifetime.getCount(type);
         long newRecentValue = value - this.lifetime.getValue(type);
         this.recent.set(newRecentValue, newRecentCount, type);
         this.lifetime.set(value, count, type);
      }

      public boolean equals(Object other) {
         return other == this?true:(!(other instanceof NodeMetricsWriter.TableLatencyMetadata)?false:((NodeMetricsWriter.TableLatencyMetadata)other).ks.equals(this.ks) && ((NodeMetricsWriter.TableLatencyMetadata)other).table.equals(this.table));
      }

      public int hashCode() {
         return this.ks.hashCode() + this.table.hashCode() * 31;
      }
   }
}
