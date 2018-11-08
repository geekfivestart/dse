package org.apache.cassandra.batchlog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.RateLimiter;
import io.reactivex.Completable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.WriteVerbs;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.hints.Hint;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingVersion;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.WrappingWriteHandler;
import org.apache.cassandra.service.WriteEndpoints;
import org.apache.cassandra.service.WriteHandler;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.Threads;
import org.apache.cassandra.utils.versioning.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchlogManager implements BatchlogManagerMBean {
   private static final WriteVerbs.WriteVersion CURRENT_VERSION = (WriteVerbs.WriteVersion)Version.last(WriteVerbs.WriteVersion.class);
   public static final String MBEAN_NAME = "org.apache.cassandra.db:type=BatchlogManager";
   private static final long REPLAY_INITIAL_DELAY;
   private static final long REPLAY_INTERVAL;
   static final int DEFAULT_PAGE_SIZE;
   private static final int MIN_CONNECTION_AGE;
   private static final Logger logger;
   public static final BatchlogManager instance;
   public static final long BATCHLOG_REPLAY_TIMEOUT;
   private static final int REQUIRED_BATCHLOG_REPLICA_COUNT;
   private volatile long totalBatchesReplayed = 0L;
   private volatile UUID lastReplayedUuid = UUIDGen.minTimeUUID(0L);
   private final ScheduledExecutorService batchlogTasks;
   private final RateLimiter rateLimiter = RateLimiter.create(1.7976931348623157E308D);

   public BatchlogManager() {
      ScheduledThreadPoolExecutor executor = new DebuggableScheduledThreadPoolExecutor("BatchlogTasks");
      executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
      this.batchlogTasks = executor;
   }

   public void start() {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

      try {
         mbs.registerMBean(this, new ObjectName("org.apache.cassandra.db:type=BatchlogManager"));
      } catch (Exception var3) {
         throw new RuntimeException(var3);
      }

      if(REPLAY_INTERVAL > 0L) {
         logger.debug("Scheduling batchlog replay initially after {} ms, then every {} ms", Long.valueOf(REPLAY_INITIAL_DELAY), Long.valueOf(REPLAY_INTERVAL));
         this.batchlogTasks.scheduleWithFixedDelay(this::replayFailedBatches, REPLAY_INITIAL_DELAY, REPLAY_INTERVAL, TimeUnit.MILLISECONDS);
      } else {
         logger.warn("Scheduled batchlog replay disabled (dse.batchlog.replay_interval_in_ms)");
      }

   }

   public void shutdown() throws InterruptedException {
      this.batchlogTasks.shutdown();
      this.batchlogTasks.awaitTermination(60L, TimeUnit.SECONDS);
   }

   public static Completable remove(UUID id) {
      if(logger.isTraceEnabled()) {
         logger.trace("Removing batch {}", id);
      }

      return (new Mutation(PartitionUpdate.fullPartitionDelete(SystemKeyspace.Batches, UUIDType.instance.decompose(id), FBUtilities.timestampMicros(), FBUtilities.nowInSeconds()))).applyAsync();
   }

   public static Completable store(Batch batch) {
      return store(batch, true);
   }

   public static Completable store(Batch batch, boolean durableWrites) {
      if(logger.isTraceEnabled()) {
         logger.trace("Storing batch {}", batch.id);
      }

      List<ByteBuffer> mutations = new ArrayList(batch.encodedMutations.size() + batch.decodedMutations.size());
      mutations.addAll(batch.encodedMutations);
      Iterator var3 = batch.decodedMutations.iterator();

      while(var3.hasNext()) {
         Mutation mutation = (Mutation)var3.next();
         mutations.add(((Mutation.MutationSerializer)Mutation.rawSerializers.get(CURRENT_VERSION.encodingVersion)).serializedBuffer(mutation));
      }

      PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(SystemKeyspace.Batches, new Object[]{batch.id});
      builder.row(new Object[0]).timestamp(batch.creationTime).add("version", Integer.valueOf(MessagingService.current_version.protocolVersion().handshakeVersion)).appendAll("mutations", mutations);
      return builder.buildAsMutation().applyAsync(durableWrites, true);
   }

   @VisibleForTesting
   public int countAllBatches() {
      String query = String.format("SELECT count(*) FROM %s.%s", new Object[]{"system", "batches"});
      UntypedResultSet results = QueryProcessor.executeInternal(query, new Object[0]);
      return results != null && !results.isEmpty()?(int)results.one().getLong("count"):0;
   }

   public long getTotalBatchesReplayed() {
      return this.totalBatchesReplayed;
   }

   public void forceBatchlogReplay() throws Exception {
      this.startBatchlogReplay().get();
   }

   public Future<?> startBatchlogReplay() {
      return this.batchlogTasks.submit(this::replayFailedBatches);
   }

   void performInitialReplay() throws InterruptedException, ExecutionException {
      this.batchlogTasks.submit(this::replayFailedBatches).get();
   }

   private void replayFailedBatches() {
      logger.trace("Started replayFailedBatches");
      int endpointsCount = StorageService.instance.getTokenMetadata().getSizeOfAllEndpoints();
      if(endpointsCount <= 0) {
         logger.trace("Replay cancelled as there are no peers in the ring.");
      } else {
         this.setRate(DatabaseDescriptor.getBatchlogReplayThrottleInKB());
         UUID limitUuid = UUIDGen.maxTimeUUID(System.currentTimeMillis() - getBatchlogTimeout());
         ColumnFamilyStore store = Keyspace.open("system").getColumnFamilyStore("batches");
         int pageSize = calculatePageSize(store);
         String query = String.format("SELECT id, mutations, version FROM %s.%s WHERE token(id) > token(?) AND token(id) <= token(?)", new Object[]{"system", "batches"});
         UntypedResultSet batches = QueryProcessor.executeInternalWithPaging(query, PageSize.rowsSize(pageSize), new Object[]{this.lastReplayedUuid, limitUuid});
         this.processBatchlogEntries(batches, pageSize, this.rateLimiter);
         this.lastReplayedUuid = limitUuid;
         logger.trace("Finished replayFailedBatches");
      }
   }

   public void setRate(int throttleInKB) {
      int endpointsCount = StorageService.instance.getTokenMetadata().getSizeOfAllEndpoints();
      if(endpointsCount > 0) {
         int endpointThrottleInKB = throttleInKB / endpointsCount;
         double throughput = endpointThrottleInKB == 0?1.7976931348623157E308D:(double)endpointThrottleInKB * 1024.0D;
         if(this.rateLimiter.getRate() != throughput) {
            logger.debug("Updating batchlog replay throttle to {} KB/s, {} KB/s per endpoint", Integer.valueOf(throttleInKB), Integer.valueOf(endpointThrottleInKB));
            this.rateLimiter.setRate(throughput);
         }
      }

   }

   static int calculatePageSize(ColumnFamilyStore store) {
      double averageRowSize = store.getMeanPartitionSize();
      return averageRowSize <= 0.0D?DEFAULT_PAGE_SIZE:(int)Math.max(1.0D, Math.min((double)DEFAULT_PAGE_SIZE, 4194304.0D / averageRowSize));
   }

   private WriteVerbs.WriteVersion getVersion(UntypedResultSet.Row row, String name) {
      int messagingVersion = row.getInt(name);
      MessagingVersion version = MessagingVersion.fromHandshakeVersion(messagingVersion);
      return (WriteVerbs.WriteVersion)version.groupVersion(Verbs.Group.WRITES);
   }

   private void processBatchlogEntries(UntypedResultSet batches, int pageSize, RateLimiter rateLimiter) {
      ArrayList<BatchlogManager.ReplayingBatch> unfinishedBatches = new ArrayList(pageSize);
      Set<InetAddress> hintedNodes = new HashSet();
      Set<UUID> replayedBatches = new HashSet();
      TPCUtils.blockingGet(Threads.observeOn(batches.rows(), TPC.ioScheduler(), TPCTaskType.BATCH_REPLAY).reduceToRxSingle(Integer.valueOf(0), (positionInPage, row) -> {
         UUID id = row.getUUID("id");
         WriteVerbs.WriteVersion version = this.getVersion(row, "version");

         try {
            BatchlogManager.ReplayingBatch batch = new BatchlogManager.ReplayingBatch(id, version, row.getList("mutations", BytesType.instance));
            if(batch.replay(rateLimiter, hintedNodes)) {
               unfinishedBatches.add(batch);
            } else {
               StorageMetrics.hintedBatchlogReplays.mark();
               if(logger.isTraceEnabled()) {
                  logger.trace("Failed to replay batchlog {} of age {} seconds with {} mutations, will write hints. Reason/failure: remote endpoints not alive", new Object[]{id, Long.valueOf(TimeUnit.MILLISECONDS.toSeconds(batch.ageInMillis())), Integer.valueOf(batch.mutations.size())});
               }

               remove(id).blockingAwait();
               ++this.totalBatchesReplayed;
            }
         } catch (IOException var11) {
            logger.warn("Skipped batch replay of {} due to {}", id, var11.getMessage());
            remove(id).blockingAwait();
         }

         if((positionInPage = Integer.valueOf(positionInPage.intValue() + 1)).intValue() == pageSize) {
            this.finishAndClearBatches(unfinishedBatches, hintedNodes, replayedBatches);
            return Integer.valueOf(0);
         } else {
            return positionInPage;
         }
      }));
      this.finishAndClearBatches(unfinishedBatches, hintedNodes, replayedBatches);
      if(!hintedNodes.isEmpty()) {
         logger.trace("Batchlog replay created hints for {} nodes", Integer.valueOf(hintedNodes.size()));
         HintsService var10000 = HintsService.instance;
         StorageService var10002 = StorageService.instance;
         StorageService.instance.getClass();
         var10000.flushAndFsyncBlockingly(Iterables.transform(hintedNodes, var10002::getHostIdForEndpoint));
      }

      replayedBatches.forEach((uuid) -> {
         remove(uuid).blockingAwait();
      });
   }

   private void finishAndClearBatches(ArrayList<BatchlogManager.ReplayingBatch> batches, Set<InetAddress> hintedNodes, Set<UUID> replayedBatches) {
      Iterator var4 = batches.iterator();

      while(var4.hasNext()) {
         BatchlogManager.ReplayingBatch batch = (BatchlogManager.ReplayingBatch)var4.next();
         batch.finish(hintedNodes);
         replayedBatches.add(batch.id);
      }

      this.totalBatchesReplayed += (long)batches.size();
      batches.clear();
   }

   public static long getBatchlogTimeout() {
      return BATCHLOG_REPLAY_TIMEOUT;
   }

   public static Collection<InetAddress> filterEndpoints(ConsistencyLevel consistencyLevel, String localRack, Multimap<String, InetAddress> localDcEndpoints) {
      BatchlogManager.EndpointFilter filter = endpointFilter(consistencyLevel, localRack, localDcEndpoints);
      return filter.filter();
   }

   @VisibleForTesting
   static BatchlogManager.EndpointFilter endpointFilter(ConsistencyLevel consistencyLevel, String localRack, Multimap<String, InetAddress> localDcEndpoints) {
      return (BatchlogManager.EndpointFilter)(DatabaseDescriptor.getBatchlogEndpointStrategy().dynamicSnitch && DatabaseDescriptor.isDynamicEndpointSnitch()?new BatchlogManager.DynamicEndpointFilter(consistencyLevel, localRack, localDcEndpoints):new BatchlogManager.RandomEndpointFilter(consistencyLevel, localRack, localDcEndpoints));
   }

   static {
      REPLAY_INITIAL_DELAY = Long.getLong("dse.batchlog.replay_initial_delay_in_ms", (long)StorageService.RING_DELAY).longValue();
      REPLAY_INTERVAL = Long.getLong("dse.batchlog.replay_interval_in_ms", 10000L).longValue();
      DEFAULT_PAGE_SIZE = Integer.getInteger("dse.batchlog.page_size", 128).intValue();
      MIN_CONNECTION_AGE = Integer.getInteger("dse.batchlog.min_connection_age_seconds", 300).intValue();
      logger = LoggerFactory.getLogger(BatchlogManager.class);
      instance = new BatchlogManager();
      BATCHLOG_REPLAY_TIMEOUT = Long.getLong("cassandra.batchlog.replay_timeout_in_ms", DatabaseDescriptor.getWriteRpcTimeout() * 2L).longValue();
      REQUIRED_BATCHLOG_REPLICA_COUNT = Math.min(2, Integer.getInteger("dse.batchlog.required_replica_count", 2).intValue());
   }

   public abstract static class EndpointFilter {
      final ConsistencyLevel consistencyLevel;
      final String localRack;
      final Multimap<String, InetAddress> endpoints;

      @VisibleForTesting
      EndpointFilter(ConsistencyLevel consistencyLevel, String localRack, Multimap<String, InetAddress> endpoints) {
         this.consistencyLevel = batchlogConsistencyLevel(consistencyLevel);
         this.localRack = localRack;
         this.endpoints = endpoints;
      }

      static ConsistencyLevel batchlogConsistencyLevel(ConsistencyLevel consistencyLevel) {
         return consistencyLevel == ConsistencyLevel.ANY?ConsistencyLevel.ANY:(BatchlogManager.REQUIRED_BATCHLOG_REPLICA_COUNT == 2?ConsistencyLevel.TWO:ConsistencyLevel.ONE);
      }

      public abstract Collection<InetAddress> filter();

      ListMultimap<String, InetAddress> validatedNodes(int endpointCount) {
         int rackCount = this.endpoints.keySet().size();
         ListMultimap<String, InetAddress> validated = ArrayListMultimap.create(rackCount, endpointCount / rackCount);
         Iterator var4 = this.endpoints.entries().iterator();

         while(var4.hasNext()) {
            Entry<String, InetAddress> entry = (Entry)var4.next();
            if(this.isValid((InetAddress)entry.getValue())) {
               validated.put(entry.getKey(), entry.getValue());
            }
         }

         return validated;
      }

      void filterLocalRack(ListMultimap<String, InetAddress> validated) {
         if(validated.size() - validated.get(this.localRack).size() >= BatchlogManager.REQUIRED_BATCHLOG_REPLICA_COUNT) {
            validated.removeAll(this.localRack);
         }

      }

      Collection<InetAddress> checkFewEndpoints(Collection<InetAddress> allEndpoints, int totalEndpointCount) {
         int available = 0;
         Iterator var4 = allEndpoints.iterator();

         while(var4.hasNext()) {
            InetAddress ep = (InetAddress)var4.next();
            if(this.isAlive(ep)) {
               ++available;
            }
         }

         allEndpoints = this.maybeThrowUnavailableException(allEndpoints, totalEndpointCount, available);
         return allEndpoints;
      }

      Collection<InetAddress> notEnoughAvailableEndpoints(ListMultimap<String, InetAddress> validated) {
         Collection<InetAddress> validatedEndpoints = validated.values();
         int validatedCount = validatedEndpoints.size();
         validatedEndpoints = this.maybeThrowUnavailableException(validatedEndpoints, BatchlogManager.REQUIRED_BATCHLOG_REPLICA_COUNT, validatedCount);
         return validatedEndpoints;
      }

      Collection<InetAddress> maybeThrowUnavailableException(Collection<InetAddress> endpoints, int totalEndpointCount, int available) {
         if(available == 0) {
            if(this.consistencyLevel == ConsistencyLevel.ANY) {
               return Collections.singleton(FBUtilities.getBroadcastAddress());
            } else {
               throw new UnavailableException("Cannot achieve consistency level " + this.consistencyLevel + " for batchlog in local DC, required:" + totalEndpointCount + ", available:" + available, this.consistencyLevel, totalEndpointCount, available);
            }
         } else {
            return endpoints;
         }
      }

      @VisibleForTesting
      protected boolean isValid(InetAddress input) {
         return !input.equals(this.getCoordinator()) && this.isAlive(input) && (!Gossiper.instance.isEnabled() || MessagingService.instance().hasValidIncomingConnections(input, BatchlogManager.MIN_CONNECTION_AGE));
      }

      @VisibleForTesting
      protected boolean isAlive(InetAddress input) {
         return FailureDetector.instance.isAlive(input);
      }

      @VisibleForTesting
      protected InetAddress getCoordinator() {
         return FBUtilities.getBroadcastAddress();
      }
   }

   public static class RandomEndpointFilter extends BatchlogManager.EndpointFilter {
      RandomEndpointFilter(ConsistencyLevel consistencyLevel, String localRack, Multimap<String, InetAddress> endpoints) {
         super(consistencyLevel, localRack, endpoints);
      }

      public Collection<InetAddress> filter() {
         Collection<InetAddress> allEndpoints = this.endpoints.values();
         int endpointCount = allEndpoints.size();
         if(endpointCount <= BatchlogManager.REQUIRED_BATCHLOG_REPLICA_COUNT) {
            return this.checkFewEndpoints(allEndpoints, endpointCount);
         } else {
            ListMultimap<String, InetAddress> validated = this.validatedNodes(endpointCount);
            int numValidated = validated.size();
            if(numValidated <= BatchlogManager.REQUIRED_BATCHLOG_REPLICA_COUNT) {
               return this.notEnoughAvailableEndpoints(validated);
            } else {
               this.filterLocalRack(validated);
               numValidated = validated.size();
               if(numValidated <= BatchlogManager.REQUIRED_BATCHLOG_REPLICA_COUNT) {
                  return this.notEnoughAvailableEndpoints(validated);
               } else {
                  List<Collection<InetAddress>> rackNodes = new ArrayList(validated.asMap().values());
                  Collections.shuffle(rackNodes, ThreadLocalRandom.current());
                  List<InetAddress> result = new ArrayList(BatchlogManager.REQUIRED_BATCHLOG_REPLICA_COUNT);
                  int i = 0;

                  while(result.size() < BatchlogManager.REQUIRED_BATCHLOG_REPLICA_COUNT) {
                     List<InetAddress> singleRack = (List)rackNodes.get(i);
                     InetAddress endpoint = (InetAddress)singleRack.get(ThreadLocalRandom.current().nextInt(singleRack.size()));
                     if(!result.contains(endpoint)) {
                        result.add(endpoint);
                        ++i;
                        if(i == rackNodes.size()) {
                           i = 0;
                        }
                     }
                  }

                  return result;
               }
            }
         }
      }
   }

   public static class DynamicEndpointFilter extends BatchlogManager.EndpointFilter {
      DynamicEndpointFilter(ConsistencyLevel consistencyLevel, String localRack, Multimap<String, InetAddress> endpoints) {
         super(consistencyLevel, localRack, endpoints);
      }

      public Collection<InetAddress> filter() {
         Collection<InetAddress> allEndpoints = this.endpoints.values();
         int endpointCount = allEndpoints.size();
         if(endpointCount <= BatchlogManager.REQUIRED_BATCHLOG_REPLICA_COUNT) {
            return this.checkFewEndpoints(allEndpoints, endpointCount);
         } else {
            ListMultimap<String, InetAddress> validated = this.validatedNodes(endpointCount);
            int numValidated = validated.size();
            if(numValidated <= BatchlogManager.REQUIRED_BATCHLOG_REPLICA_COUNT) {
               return this.notEnoughAvailableEndpoints(validated);
            } else {
               if(!DatabaseDescriptor.getBatchlogEndpointStrategy().allowLocalRack) {
                  this.filterLocalRack(validated);
                  numValidated = validated.size();
                  if(numValidated <= BatchlogManager.REQUIRED_BATCHLOG_REPLICA_COUNT) {
                     return this.notEnoughAvailableEndpoints(validated);
                  }
               }

               List<InetAddress> sorted = this.reorder(validated.values());
               List<InetAddress> result = new ArrayList(BatchlogManager.REQUIRED_BATCHLOG_REPLICA_COUNT);
               HashSet racks = new HashSet();

               while(result.size() < BatchlogManager.REQUIRED_BATCHLOG_REPLICA_COUNT) {
                  Iterator var8 = sorted.iterator();

                  while(var8.hasNext()) {
                     InetAddress endpoint = (InetAddress)var8.next();
                     if(result.size() == BatchlogManager.REQUIRED_BATCHLOG_REPLICA_COUNT) {
                        break;
                     }

                     if(racks.isEmpty()) {
                        racks.addAll(validated.keySet());
                     }

                     String rack = DatabaseDescriptor.getEndpointSnitch().getRack(endpoint);
                     if(racks.remove(rack) && !result.contains(endpoint)) {
                        result.add(endpoint);
                     }
                  }
               }

               return result;
            }
         }
      }

      List<InetAddress> reorder(Collection<InetAddress> endpoints) {
         return DatabaseDescriptor.getEndpointSnitch().getSortedListByProximity(this.getCoordinator(), endpoints);
      }
   }

   private static class ReplayingBatch {
      private final UUID id;
      private final long writtenAt;
      private final List<Mutation> mutations;
      private final int replayedBytes;
      private List<BatchlogManager.ReplayingBatch.ReplayWriteHandler> replayHandlers;

      ReplayingBatch(UUID id, WriteVerbs.WriteVersion version, List<ByteBuffer> serializedMutations) throws IOException {
         this.id = id;
         this.writtenAt = UUIDGen.unixTimestamp(id);
         this.mutations = new ArrayList(serializedMutations.size());
         this.replayedBytes = this.addMutations(version, serializedMutations);
      }

      public long ageInMillis() {
         return System.currentTimeMillis() - UUIDGen.getAdjustedTimestamp(this.id);
      }

      public boolean replay(RateLimiter rateLimiter, Set<InetAddress> hintedNodes) throws IOException {
         BatchlogManager.logger.trace("Replaying batch {}", this.id);
         if(this.mutations.isEmpty()) {
            return false;
         } else {
            int gcgs = gcgs(this.mutations);
            if(TimeUnit.MILLISECONDS.toSeconds(this.writtenAt) + (long)gcgs <= (long)FBUtilities.nowInSeconds()) {
               return false;
            } else {
               this.replayHandlers = sendReplays(this.mutations, this.writtenAt, hintedNodes);
               rateLimiter.acquire(this.replayedBytes);
               return !this.replayHandlers.isEmpty();
            }
         }
      }

      public void finish(Set<InetAddress> hintedNodes) {
         boolean replayFailed = false;

         for(int i = 0; i < this.replayHandlers.size(); ++i) {
            BatchlogManager.ReplayingBatch.ReplayWriteHandler handler = (BatchlogManager.ReplayingBatch.ReplayWriteHandler)this.replayHandlers.get(i);

            try {
               handler.get();
            } catch (WriteFailureException | WriteTimeoutException var6) {
               this.writeHintsForUndeliveredEndpoints(i, hintedNodes);
               if(!replayFailed) {
                  replayFailed = true;
                  StorageMetrics.hintedBatchlogReplays.mark();
                  if(BatchlogManager.logger.isTraceEnabled()) {
                     BatchlogManager.logger.trace("Failed to replay batchlog {} of age {} seconds with {} mutations, will write hints. Reason/failure: {}", new Object[]{this.id, Long.valueOf(TimeUnit.MILLISECONDS.toSeconds(this.ageInMillis())), Integer.valueOf(this.mutations.size()), var6.getMessage()});
                  }
               }
            }
         }

         if(!replayFailed) {
            if(BatchlogManager.logger.isTraceEnabled()) {
               BatchlogManager.logger.trace("Finished replay of batchlog {} of age {} seconds with {} mutations", new Object[]{this.id, Long.valueOf(TimeUnit.MILLISECONDS.toSeconds(this.ageInMillis())), Integer.valueOf(this.mutations.size())});
            }

            StorageMetrics.batchlogReplays.mark();
         }
      }

      private int addMutations(WriteVerbs.WriteVersion version, List<ByteBuffer> serializedMutations) throws IOException {
         int ret = 0;
         Iterator var4 = serializedMutations.iterator();

         while(var4.hasNext()) {
            ByteBuffer serializedMutation = (ByteBuffer)var4.next();
            ret += serializedMutation.remaining();
            DataInputBuffer in = new DataInputBuffer(serializedMutation, true);
            Throwable var7 = null;

            try {
               this.addMutation((Mutation)((Serializer)Mutation.serializers.get(version)).deserialize(in));
            } catch (Throwable var16) {
               var7 = var16;
               throw var16;
            } finally {
               if(in != null) {
                  if(var7 != null) {
                     try {
                        in.close();
                     } catch (Throwable var15) {
                        var7.addSuppressed(var15);
                     }
                  } else {
                     in.close();
                  }
               }

            }
         }

         return ret;
      }

      private void addMutation(Mutation mutation) {
         Iterator var2 = mutation.getTableIds().iterator();

         while(var2.hasNext()) {
            TableId tableId = (TableId)var2.next();
            if(this.writtenAt <= SystemKeyspace.getTruncatedAt(tableId)) {
               mutation = mutation.without(tableId);
            }
         }

         if(!mutation.isEmpty()) {
            this.mutations.add(mutation);
         }

      }

      private void writeHintsForUndeliveredEndpoints(int index, Set<InetAddress> hintedNodes) {
         int gcgs = ((Mutation)this.mutations.get(index)).smallestGCGS();
         if(TimeUnit.MILLISECONDS.toSeconds(this.writtenAt) + (long)gcgs > (long)FBUtilities.nowInSeconds()) {
            BatchlogManager.ReplayingBatch.ReplayWriteHandler handler = (BatchlogManager.ReplayingBatch.ReplayWriteHandler)this.replayHandlers.get(index);
            Mutation undeliveredMutation = (Mutation)this.mutations.get(index);
            if(handler != null) {
               if(BatchlogManager.logger.isTraceEnabled()) {
                  BatchlogManager.logger.trace("Adding hints for undelivered endpoints: {}", handler.undelivered);
               }

               hintedNodes.addAll(handler.undelivered);
               HintsService var10000 = HintsService.instance;
               Set var10001 = handler.undelivered;
               StorageService var10002 = StorageService.instance;
               StorageService.instance.getClass();
               var10000.write(Iterables.transform(var10001, var10002::getHostIdForEndpoint), Hint.create(undeliveredMutation, this.writtenAt));
            }

         }
      }

      private static List<BatchlogManager.ReplayingBatch.ReplayWriteHandler> sendReplays(List<Mutation> mutations, long writtenAt, Set<InetAddress> hintedNodes) {
         List<BatchlogManager.ReplayingBatch.ReplayWriteHandler> handlers = new ArrayList(mutations.size());
         Iterator var5 = mutations.iterator();

         while(var5.hasNext()) {
            Mutation mutation = (Mutation)var5.next();
            BatchlogManager.ReplayingBatch.ReplayWriteHandler handler = sendSingleReplayMutation(mutation, writtenAt, hintedNodes);
            if(handler != null) {
               handlers.add(handler);
            }
         }

         return handlers;
      }

      private static BatchlogManager.ReplayingBatch.ReplayWriteHandler sendSingleReplayMutation(Mutation mutation, long writtenAt, Set<InetAddress> hintedNodes) {
         WriteEndpoints endpoints = WriteEndpoints.compute((IMutation)mutation);
         Iterator var5 = endpoints.dead().iterator();

         while(var5.hasNext()) {
            InetAddress dead = (InetAddress)var5.next();
            hintedNodes.add(dead);
            HintsService.instance.write(StorageService.instance.getHostIdForEndpoint(dead), Hint.create(mutation, writtenAt));
         }

         WriteEndpoints writeEndpoints = endpoints;
         if(endpoints.live().contains(FBUtilities.getBroadcastAddress())) {
            mutation.apply();
            if(endpoints.live().size() == 1) {
               return null;
            }

            writeEndpoints = endpoints.withoutLocalhost(true);
         }

         if(writeEndpoints.liveCount() == 0) {
            return null;
         } else {
            BatchlogManager.ReplayingBatch.ReplayWriteHandler handler = BatchlogManager.ReplayingBatch.ReplayWriteHandler.create(writeEndpoints, System.nanoTime());
            String localDataCenter = DatabaseDescriptor.getLocalDataCenter();
            Verb.AckedRequest<Mutation> messageDefinition = Verbs.WRITES.WRITE;
            MessagingService.instance().send((Request.Dispatcher)messageDefinition.newForwardingDispatcher(writeEndpoints.live(), localDataCenter, mutation), handler);
            return handler;
         }
      }

      private static int gcgs(Collection<Mutation> mutations) {
         int gcgs = 2147483647;

         Mutation mutation;
         for(Iterator var2 = mutations.iterator(); var2.hasNext(); gcgs = Math.min(gcgs, mutation.smallestGCGS())) {
            mutation = (Mutation)var2.next();
         }

         return gcgs;
      }

      private static class ReplayWriteHandler extends WrappingWriteHandler {
         private final Set<InetAddress> undelivered = Collections.newSetFromMap(new ConcurrentHashMap());

         private ReplayWriteHandler(WriteHandler handler) {
            super(handler);
            Iterables.addAll(this.undelivered, handler.endpoints());
         }

         static BatchlogManager.ReplayingBatch.ReplayWriteHandler create(WriteEndpoints endpoints, long queryStartNanos) {
            WriteHandler handler = WriteHandler.builder(endpoints, ConsistencyLevel.ALL, WriteType.UNLOGGED_BATCH, queryStartNanos, TPC.bestTPCTimer()).blockFor(endpoints.liveCount()).build();
            return new BatchlogManager.ReplayingBatch.ReplayWriteHandler(handler);
         }

         public void onResponse(Response<EmptyPayload> m) {
            boolean removed = this.undelivered.remove(m.from());

            assert removed : "did not expect a response from " + m.from() + " / expected are " + this.endpoints().live();

            super.onResponse(m);
         }
      }
   }
}
