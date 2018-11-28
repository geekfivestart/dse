package com.datastax.bdp.db.audit;

import com.datastax.bdp.db.upgrade.SchemaUpgrade;
import com.datastax.bdp.db.upgrade.VersionDependentFeature;
import com.google.common.annotations.VisibleForTesting;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCScheduler;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;
import org.jctools.queues.MpmcArrayQueue;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraAuditWriter implements IAuditWriter {
   private static final Logger logger = LoggerFactory.getLogger(CassandraAuditWriter.class);
   private static final String DROPPED_EVENT_LOGGER = "DroppedAuditEventLogger";
   private static final Logger droppedEventLogger = LoggerFactory.getLogger("DroppedAuditEventLogger");
   static int NUM_FIELDS = 14;
   private static final ByteBuffer nodeIp = ByteBufferUtil.bytes(FBUtilities.getBroadcastAddress());
   private long startupTime;
   private final int retentionPeriod;
   private final Queue<CassandraAuditWriter.EventBindings> eventQueue;
   private final ConsistencyLevel writeConsistency;
   @VisibleForTesting
   final CassandraAuditWriter.BatchingOptions batchingOptions;
   private final AtomicReference<CassandraAuditWriter.State> state;
   private volatile CassandraAuditWriter.EventPartition lastPartition;
   @VisibleForTesting
   final VersionDependentFeature<CassandraAuditWriter.VersionDepentent> feature;

   public boolean isSetUpComplete() {
      return this.state.get() == CassandraAuditWriter.State.READY;
   }

   public void setUp() {
      if(this.state.compareAndSet(CassandraAuditWriter.State.NOT_STARTED, CassandraAuditWriter.State.STARTING)) {
         CassandraAuditKeyspace.maybeConfigure();
         this.feature.setup(Gossiper.instance.clusterVersionBarrier);
         this.startupTime = System.currentTimeMillis();
         if(!this.batchingOptions.synchronous) {
            this.scheduleNextFlush();
         }

         Runtime.getRuntime().addShutdownHook(new Thread(new WrappedRunnable() {
            protected void runMayThrow() {
               CassandraAuditWriter.logger.debug("Shutting down CassandraAuditWritter");
               CassandraAuditWriter.this.state.compareAndSet(CassandraAuditWriter.State.READY, CassandraAuditWriter.State.STOPPING);
            }
         }, "Audit shutdown"));
         this.state.set(CassandraAuditWriter.State.READY);
      }

   }

   private void scheduleNextFlush() {
      if(!this.state.compareAndSet(CassandraAuditWriter.State.STOPPING, CassandraAuditWriter.State.STOPPED)) {
         long currentTimeMillis = System.currentTimeMillis();
         long nextFlushTime = this.nextFlushAfter(currentTimeMillis);
         long delay = nextFlushTime - currentTimeMillis;
         this.flushTask().doOnError((e) -> {
            logger.error("Audit logging batching task failed.", e);
         }).doFinally(() -> {
            this.scheduleNextFlush();
         }).delaySubscription(delay, TimeUnit.MILLISECONDS, this.getTpcSchedulerForNextFlush()).subscribe();
      }

   }

   private TPCScheduler getTpcSchedulerForNextFlush() {
      return this.lastPartition == null?TPC.bestTPCScheduler():TPC.getForKey(CassandraAuditKeyspace.getKeyspace(), this.lastPartition.getPartitionKey(CassandraAuditKeyspace.getAuditLogPartitioner()));
   }

   private long nextFlushAfter(long currentTimeMillis) {
      long floor = floor(currentTimeMillis, this.startupTime, this.batchingOptions.flushPeriodInMillis);
      return floor + (long)this.batchingOptions.flushPeriodInMillis;
   }

   private static long floor(long currentTime, long startupTime, int period) {
      long delta = (currentTime - startupTime) % (long)period;
      return currentTime - delta;
   }

   private Single<Boolean> flushTask() {
      return Single.fromCallable(new Callable<Boolean>() {
         public Boolean call() throws Exception {
            Map<CassandraAuditWriter.EventPartition, CassandraAuditWriter.EventBatch> batches = new HashMap();
            int numberOfEvents = 0;

            do {
               CassandraAuditWriter.EventBindings event = (CassandraAuditWriter.EventBindings)CassandraAuditWriter.this.eventQueue.poll();
               if(event == null) {
                  break;
               }

               ++numberOfEvents;
               CassandraAuditWriter.EventBatch batch = (CassandraAuditWriter.EventBatch)batches.get(event.partition);
               if(batch == null) {
                  batch = CassandraAuditWriter.this.new EventBatch(event.partition);
                  batches.put(event.partition, batch);
               }

               batch.addEvent(event);
               if(batch.size() >= CassandraAuditWriter.this.batchingOptions.batchSize) {
                  CassandraAuditWriter.this.executeBatches(Collections.singletonList(batch));
                  batches.remove(event.partition);
               }
            } while(numberOfEvents < CassandraAuditWriter.this.batchingOptions.queueSize);

            CassandraAuditWriter.this.executeBatches(batches.values());
            return Boolean.TRUE;
         }
      });
   }

   @VisibleForTesting
   protected void executeBatches(Collection<CassandraAuditWriter.EventBatch> batches) {
      Iterator var2 = batches.iterator();

      while(var2.hasNext()) {
         CassandraAuditWriter.EventBatch batch = (CassandraAuditWriter.EventBatch)var2.next();
         batch.execute();
      }

   }

   public CassandraAuditWriter() {
      this(DatabaseDescriptor.getAuditLoggingOptions().retention_time, DatabaseDescriptor.getRawConfig().getAuditCassConsistencyLevel(), DatabaseDescriptor.getRawConfig().getAuditLoggerCassMode().equals("sync")?CassandraAuditWriter.BatchingOptions.SYNC:new CassandraAuditWriter.BatchingOptions(DatabaseDescriptor.getRawConfig().getAuditCassBatchSize(), DatabaseDescriptor.getRawConfig().getAuditCassFlushTime(), DatabaseDescriptor.getRawConfig().getAuditLoggerCassAsyncQueueSize()));
   }

   @VisibleForTesting
   CassandraAuditWriter(int retentionPeriod, ConsistencyLevel cl, CassandraAuditWriter.BatchingOptions batchingOptions) {
      this.state = new AtomicReference(CassandraAuditWriter.State.NOT_STARTED);
      this.feature = VersionDependentFeature.createForSchemaUpgrade("CassandraAuditWriter", new CassandraVersion("3.11.0"), new CassandraAuditWriter.Legacy(), new CassandraAuditWriter.Native(), new SchemaUpgrade(CassandraAuditKeyspace.metadata(), CassandraAuditKeyspace.tablesIfNotExist(), true), logger, "Preparing upgrade to DSE 5.1/6.0 audit log entries.", "Unlocking DSE 5.1/6.0 audit log entries.", "Using DSE 5.0 compatible audit log entries - DSE 5.1 compatible audit log entries will be written when all nodes are on DSE 5.1 or newer and automatic schema upgrade has finished.");
      this.retentionPeriod = (int)TimeUnit.SECONDS.convert((long)retentionPeriod, TimeUnit.HOURS);
      this.writeConsistency = cl;
      this.batchingOptions = batchingOptions;
      this.eventQueue = (Queue)(batchingOptions.synchronous?null:(batchingOptions.queueSize > 0?new MpmcArrayQueue(batchingOptions.queueSize):new ConcurrentLinkedQueue()));
   }

   private static CQLStatement prepareStatement(String cql) {
      try {
         QueryState queryState = QueryState.forInternalCalls();
         return QueryProcessor.getStatement(cql, queryState).statement;
      } catch (Exception var2) {
         throw new IllegalStateException("Error preparing audit writer", var2);
      }
   }

   public Completable recordEvent(AuditableEvent event) {
      return ((CassandraAuditWriter.VersionDepentent)this.feature.implementation()).recordEvent(event);
   }

   private boolean isSynchronous() {
      return this.batchingOptions.synchronous;
   }

   class EventBatch {
      final CassandraAuditWriter.EventPartition partition;
      final List<ModificationStatement> modifications = new ArrayList();
      final List<List<ByteBuffer>> values = new ArrayList();
      final List<AuditableEvent> events = new ArrayList();

      public EventBatch(CassandraAuditWriter.EventPartition partition) {
         this.partition = partition;
      }

      void addEvent(CassandraAuditWriter.EventBindings event) {
         this.modifications.add(event.stmt);
         this.values.add(event.bindings);
         this.events.add(event.event);
      }

      int size() {
         return this.modifications.size();
      }

      void execute() {
         BatchStatement stmt = new BatchStatement(0, BatchStatement.Type.UNLOGGED, this.modifications, Attributes.none());
         CassandraAuditWriter.this.lastPartition = this.partition;

         try {
            this.executeBatch(stmt, CassandraAuditWriter.this.writeConsistency, this.values).doOnError((e) -> {
               this.logDroppedEvents(e);
            }).subscribe();
         } catch (RequestValidationException | RequestExecutionException var3) {
            this.logDroppedEvents(var3);
         }

      }

      private void logDroppedEvents(Throwable e) {
         CassandraAuditWriter.logger.error("Exception writing audit events to table", e);
         Iterator var2 = this.events.iterator();

         while(var2.hasNext()) {
            AuditableEvent event = (AuditableEvent)var2.next();
            CassandraAuditWriter.droppedEventLogger.warn(event.toString());
         }

      }

      private Single<ResultMessage> executeBatch(BatchStatement statement, ConsistencyLevel cl, List<List<ByteBuffer>> values) {
         BatchQueryOptions options = BatchQueryOptions.withPerStatementVariables(QueryOptions.forInternalCalls(cl, Collections.emptyList()), values, Collections.emptyList());
         return QueryProcessor.instance.processBatch(statement, QueryState.forInternalCalls(), options, System.nanoTime());
      }
   }

   private class EventBindings {
      private final ModificationStatement stmt;
      private final AuditableEvent event;
      private final CassandraAuditWriter.EventPartition partition;
      private final List<ByteBuffer> bindings;

      private EventBindings(ModificationStatement stmt, AuditableEvent event, CassandraAuditWriter.VersionDepentent versionDepentent) {
         assert event != null;

         this.stmt = stmt;
         this.event = event;
         this.partition = new CassandraAuditWriter.EventPartition(event);
         this.bindings = versionDepentent.newEventBindings(event, this.partition);
      }

      private QueryOptions getBindings() {
         return QueryOptions.forInternalCalls(CassandraAuditWriter.this.writeConsistency, this.bindings);
      }
   }

   private static class EventPartition {
      private final Date date;
      private final int dayPartition;

      private EventPartition(AuditableEvent event) {
         DateTime eventTime = new DateTime(event.getTimestamp(), DateTimeZone.UTC);
         this.date = eventTime.toDateMidnight().toDate();
         this.dayPartition = eventTime.getHourOfDay() * 60 * 60;
      }

      public DecoratedKey getPartitionKey(IPartitioner partitioner) {
         return partitioner.decorateKey(CompositeType.build(new ByteBuffer[]{TimestampType.instance.decompose(this.date), CassandraAuditWriter.nodeIp, ByteBufferUtil.bytes(this.dayPartition)}));
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            CassandraAuditWriter.EventPartition that = (CassandraAuditWriter.EventPartition)o;
            return this.dayPartition == that.dayPartition && this.date.equals(that.date);
         } else {
            return false;
         }
      }

      public int hashCode() {
         int result = this.date.hashCode();
         result = 31 * result + this.dayPartition;
         return result;
      }

      public String toString() {
         return "EventPartition{date=" + this.date + ", dayPartition=" + this.dayPartition + '}';
      }
   }

   @VisibleForTesting
   static class BatchingOptions {
      static CassandraAuditWriter.BatchingOptions SYNC = new CassandraAuditWriter.BatchingOptions(true, 1, 0, 0);
      public final boolean synchronous;
      public final int batchSize;
      public final int flushPeriodInMillis;
      public final int queueSize;

      public BatchingOptions(int batchSize, int flushPeriodInMillis, int queueSize) {
         this(false, batchSize, flushPeriodInMillis, queueSize);
      }

      private BatchingOptions(boolean synchronous, int batchSize, int flushPeriodInMillis, int queueSize) {
         this.synchronous = synchronous;
         this.batchSize = batchSize;
         this.flushPeriodInMillis = flushPeriodInMillis;
         this.queueSize = queueSize;
      }
   }

   private final class Native extends CassandraAuditWriter.VersionDepentent {
      private Native() {
         super();
      }

      String insertStatement() {
         return String.format("INSERT INTO %s.%s (date, node, day_partition, event_time, batch_id, category, keyspace_name, operation, source, table_name, type, username,authenticated,consistency) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", new Object[]{"dse_audit", "audit_log"});
      }

      List<ByteBuffer> newEventBindings(AuditableEvent event, CassandraAuditWriter.EventPartition partition) {
         List<ByteBuffer> bindings = this.addEventBindingsShared(event, partition);
         bindings.add(event.getAuthenticated() == null?null:ByteBufferUtil.bytes(event.getAuthenticated()));
         bindings.add(event.getConsistencyLevel() == null?null:ByteBufferUtil.bytes(event.getConsistencyLevel().toString()));
         if(CassandraAuditWriter.this.retentionPeriod > 0) {
            bindings.add(ByteBufferUtil.bytes(CassandraAuditWriter.this.retentionPeriod));
         }

         return bindings;
      }
   }

   private final class Legacy extends CassandraAuditWriter.VersionDepentent {
      private Legacy() {
         super();
      }

      String insertStatement() {
         return String.format("INSERT INTO %s.%s (date, node, day_partition, event_time, batch_id, category, keyspace_name, operation, source, table_name, type, username) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", new Object[]{"dse_audit", "audit_log"});
      }

      List<ByteBuffer> newEventBindings(AuditableEvent event, CassandraAuditWriter.EventPartition partition) {
         List<ByteBuffer> bindings = this.addEventBindingsShared(event, partition);
         if(CassandraAuditWriter.this.retentionPeriod > 0) {
            bindings.add(ByteBufferUtil.bytes(CassandraAuditWriter.this.retentionPeriod));
         }

         return bindings;
      }
   }

   private abstract class VersionDepentent implements VersionDependentFeature.VersionDependent {
      protected ModificationStatement insertStatement;

      private VersionDepentent() {
      }

      public void initialize() {
         boolean hasTTL = CassandraAuditWriter.this.retentionPeriod > 0;
         String cql = this.insertStatement();
         if(hasTTL) {
            cql = cql + " USING TTL ?";
         }

         this.insertStatement = (ModificationStatement)CassandraAuditWriter.prepareStatement(cql);
      }

      abstract String insertStatement();

      final Completable recordEvent(AuditableEvent event) {
         ModificationStatement stmt = this.insertStatement;
         CassandraAuditWriter.EventBindings bindings = CassandraAuditWriter.this.new EventBindings(stmt, event, this);
         if(CassandraAuditWriter.this.isSynchronous()) {
            return stmt.execute(QueryState.forInternalCalls(), bindings.getBindings(), System.nanoTime()).toCompletable();
         } else {
            if(!CassandraAuditWriter.this.eventQueue.add(bindings)) {
               CassandraAuditWriter.droppedEventLogger.warn(event.toString());
            }

            return Completable.complete();
         }
      }

      abstract List<ByteBuffer> newEventBindings(AuditableEvent var1, CassandraAuditWriter.EventPartition var2);

      List<ByteBuffer> addEventBindingsShared(AuditableEvent event, CassandraAuditWriter.EventPartition partition) {
         List<ByteBuffer> bindings = new ArrayList(CassandraAuditWriter.NUM_FIELDS + (CassandraAuditWriter.this.retentionPeriod > 0?1:0));
         bindings.add(TimestampType.instance.decompose(partition.date));
         bindings.add(CassandraAuditWriter.nodeIp);
         bindings.add(ByteBufferUtil.bytes(partition.dayPartition));
         bindings.add(ByteBufferUtil.bytes(event.getUid()));
         UUID batch_id = event.getBatchId();
         bindings.add(batch_id != null?ByteBufferUtil.bytes(batch_id):null);
         bindings.add(ByteBufferUtil.bytes(event.getType().getCategory().toString()));
         bindings.add(event.getKeyspace() != null?ByteBufferUtil.bytes(event.getKeyspace()):null);
         String operation = event.getOperation();
         bindings.add(operation != null?ByteBufferUtil.bytes(operation):null);
         String source = event.getSource();
         bindings.add(source != null?ByteBufferUtil.bytes(source):null);
         String cf = event.getColumnFamily();
         bindings.add(cf != null?ByteBufferUtil.bytes(cf):null);
         bindings.add(ByteBufferUtil.bytes(event.getType().toString()));
         bindings.add(ByteBufferUtil.bytes(event.getUser()));
         return bindings;
      }
   }

   private static enum State {
      NOT_STARTED,
      STARTING,
      READY,
      STOPPING,
      STOPPED;

      private State() {
      }
   }
}
