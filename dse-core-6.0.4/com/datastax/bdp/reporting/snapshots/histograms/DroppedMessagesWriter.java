package com.datastax.bdp.reporting.snapshots.histograms;

import com.codahale.metrics.Metered;
import com.codahale.metrics.Snapshot;
import com.datastax.bdp.gms.DseVersionNotifier;
import com.datastax.bdp.reporting.CqlWritable;
import com.datastax.bdp.reporting.CqlWriter;
import com.datastax.bdp.util.QueryProcessorUtil;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.validation.constraints.NotNull;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.BatchStatement.Type;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.metrics.DroppedMessageMetrics;
import org.apache.cassandra.metrics.Timer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.DroppedMessages.Group;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DroppedMessagesWriter extends CqlWriter<DroppedMessagesWriter.WritableDroppedMessages> implements Runnable {
   private static final Logger logger = LoggerFactory.getLogger(DroppedMessagesWriter.class);

   public DroppedMessagesWriter(InetAddress nodeAddress, int ttlSeconds) {
      super(nodeAddress, ttlSeconds);
   }

   protected String getTableName() {
      return "dropped_messages";
   }

   protected String getInsertCQL() {
      return String.format("INSERT INTO %s.%s (node_ip, histogram_id, verb, global_count, global_mean_rate, global_1min_rate, global_5min_rate, global_15min_rate, internal_count, internal_mean_rate, internal_1min_rate, internal_5min_rate, internal_15min_rate, internal_latency_median, internal_latency_p75, internal_latency_p90, internal_latency_p95, internal_latency_p98, internal_latency_p99, internal_latency_min, internal_latency_mean, internal_latency_max, internal_latency_stdev, xnode_count, xnode_mean_rate, xnode_1min_rate, xnode_5min_rate, xnode_15min_rate, xnode_median, xnode_p75, xnode_p90, xnode_p95, xnode_p98, xnode_p99, xnode_min, xnode_mean, xnode_max, xnode_stdev) VALUES (?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?) USING TTL ?", new Object[]{"dse_perf", "dropped_messages"});
   }

   protected CqlWriter<DroppedMessagesWriter.WritableDroppedMessages>.WriterConfig createWriterConfig(CassandraVersion dseVersion) {
      return dseVersion.compareTo(DseVersionNotifier.VERSION_51) >= 0?super.createWriterConfig(dseVersion):null;
   }

   public void write(@NotNull DroppedMessagesWriter.WritableDroppedMessages droppedMessages) {
      CqlWriter<DroppedMessagesWriter.WritableDroppedMessages>.WriterConfig config = this.getWriterConfig();
      if(config != null) {
         if(droppedMessages.isNotEmpty()) {
            try {
               ByteBuffer timestamp = TimestampType.instance.decompose(new Date(System.currentTimeMillis()));
               List<ModificationStatement> statements = new ArrayList();
               List<List<ByteBuffer>> allVariables = droppedMessages.getAllVariables();
               ModificationStatement stmt = (ModificationStatement)config.getInsertStatement();
               Iterator var7 = allVariables.iterator();

               while(var7.hasNext()) {
                  List<ByteBuffer> statVars = (List)var7.next();
                  statVars.add(0, timestamp);
                  statVars.add(0, this.nodeAddressBytes);
                  statVars.add(this.getTtlBytes());
                  statements.add(stmt);
               }

               QueryProcessorUtil.processBatchBlocking(new BatchStatement(-1, Type.UNLOGGED, statements, Attributes.none()), ConsistencyLevel.ONE, allVariables);
            } catch (Exception var9) {
               handleWriteException(this.getTableName(), var9);
            }
         }
      } else {
         logger.trace("Skipping write to {} because is it not yet setup", this.getTableName());
      }

   }

   public void run() {
      this.write(new DroppedMessagesWriter.WritableDroppedMessages(MessagingService.instance().getDroppedMessagesWithAllMetrics()));
   }

   protected List<ByteBuffer> getVariables(DroppedMessagesWriter.WritableDroppedMessages droppedMessages) {
      throw new UnsupportedOperationException();
   }

   public static class WritableDroppedMessages implements CqlWritable {
      private final Map<Group, DroppedMessageMetrics> metricsMap;

      public WritableDroppedMessages(Map<Group, DroppedMessageMetrics> metricsMap) {
         this.metricsMap = Collections.unmodifiableMap(metricsMap);
      }

      public boolean isNotEmpty() {
         return this.metricsMap.keySet().stream().map(this.metricsMap::get).anyMatch(this::isNotEmpty);
      }

      public boolean isNotEmpty(DroppedMessageMetrics droppedMessageMetrics) {
         return droppedMessageMetrics.dropped.getCount() > 0L || droppedMessageMetrics.internalDroppedLatency.getCount() > 0L || droppedMessageMetrics.crossNodeDroppedLatency.getCount() > 0L;
      }

      public DroppedMessageMetrics getDroppedMessageMetrics(Group group) {
         return (DroppedMessageMetrics)this.metricsMap.get(group);
      }

      public List<List<ByteBuffer>> getAllVariables() {
         return (List)this.metricsMap.keySet().stream().filter((group) -> {
            return this.isNotEmpty((DroppedMessageMetrics)this.metricsMap.get(group));
         }).map((group) -> {
            return this.getDroppedMessageMetricsVariables(group, (DroppedMessageMetrics)this.metricsMap.get(group));
         }).collect(Collectors.toList());
      }

      private List<ByteBuffer> getDroppedMessageMetricsVariables(Group group, DroppedMessageMetrics droppedMessageMetrics) {
         List<ByteBuffer> result = new ArrayList(36);
         result.add(ByteBufferUtil.bytes(group.name()));
         this.addMeteredVariables(result, droppedMessageMetrics.dropped);
         this.addTimerVariables(result, droppedMessageMetrics.internalDroppedLatency);
         this.addTimerVariables(result, droppedMessageMetrics.crossNodeDroppedLatency);
         return result;
      }

      private void addMeteredVariables(Collection<ByteBuffer> variables, Metered metered) {
         variables.add(ByteBufferUtil.bytes(metered.getCount()));
         variables.add(ByteBufferUtil.bytes(metered.getMeanRate()));
         variables.add(ByteBufferUtil.bytes(metered.getOneMinuteRate()));
         variables.add(ByteBufferUtil.bytes(metered.getFiveMinuteRate()));
         variables.add(ByteBufferUtil.bytes(metered.getFifteenMinuteRate()));
      }

      private void addTimerVariables(Collection<ByteBuffer> variables, Timer timer) {
         this.addMeteredVariables(variables, timer);
         Snapshot snapshot = timer.getSnapshot();
         variables.add(ByteBufferUtil.bytes(snapshot.getMedian()));
         variables.add(ByteBufferUtil.bytes(snapshot.get75thPercentile()));
         variables.add(ByteBufferUtil.bytes(snapshot.getValue(0.9D)));
         variables.add(ByteBufferUtil.bytes(snapshot.get95thPercentile()));
         variables.add(ByteBufferUtil.bytes(snapshot.get98thPercentile()));
         variables.add(ByteBufferUtil.bytes(snapshot.get99thPercentile()));
         variables.add(ByteBufferUtil.bytes(snapshot.getMin()));
         variables.add(ByteBufferUtil.bytes(snapshot.getMean()));
         variables.add(ByteBufferUtil.bytes(snapshot.getMax()));
         variables.add(ByteBufferUtil.bytes(snapshot.getStdDev()));
      }
   }
}
