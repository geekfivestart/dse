package com.datastax.bdp.reporting.snapshots.node;

import com.datastax.bdp.gms.DseVersionNotifier;
import com.datastax.bdp.reporting.CqlWritable;
import com.datastax.bdp.server.system.BatchlogInfoProvider;
import com.datastax.bdp.server.system.CacheInfoProvider;
import com.datastax.bdp.server.system.ClientRequestMetricsProvider;
import com.datastax.bdp.server.system.CommitLogInfoProvider;
import com.datastax.bdp.server.system.CompactionInfoProvider;
import com.datastax.bdp.server.system.GCInfoProvider;
import com.datastax.bdp.server.system.MessagingInfoProvider;
import com.datastax.bdp.server.system.StorageInfoProvider;
import com.datastax.bdp.server.system.StreamInfoProvider;
import com.datastax.bdp.server.system.SystemResourcesInfoProvider;
import com.datastax.bdp.server.system.ThreadPoolStats;
import com.datastax.bdp.server.system.ThreadPoolStatsProvider;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.net.DroppedMessages.Group;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeInfo implements CqlWritable {
   private static final Logger logger = LoggerFactory.getLogger(NodeInfo.class);
   public static final long NON_EXISTING = -1L;
   public final InetAddress address;
   public final String state;
   public final long uptime;
   public final Set<String> tokens;
   public final float ownership;
   public final String datacenter;
   public final String rack;
   public final long totalReads;
   public final long totalRangeSlices;
   public final long totalWrites;
   public final double meanReadLatency;
   public final double meanRangeSliceLatency;
   public final double meanWriteLatency;
   public final long readTimeouts;
   public final long rangeSliceTimeouts;
   public final long writeTimeouts;
   public final long heapMax;
   public final long heapUsed;
   public final long cmsCollectionCount;
   public final long cmsCollectionTime;
   public final long parnewCollectionCount;
   public final long parnewCollectionTime;
   public final long compactionsCompleted;
   public final int compactionsPending;
   public final long readsPending;
   public final long mutationsPending;
   public final long readRepairPending;
   public final long repairPending;
   public final long gossipPending;
   public final long hintedHandoffPending;
   public final long internalResponsesPending;
   public final long migrationsPending;
   public final long miscTasksPending;
   public final long requestResponsesPending;
   public final long flushwritersPending;
   public final long memtablePostFlushPending;
   public final long replicateOnWritePending;
   public final int currentStreams;
   public final long totalDiskSpace;
   public final long freeDiskSpace;
   public final long totalTableDataSize;
   public final long totalIndexDataSize;
   public final long totalPhysicalMemory;
   public final double processCpuLoad;
   public final long totalBatchesReplayed;
   public final long keyCacheCapacity;
   public final long keyCacheEntries;
   public final long keyCacheSize;
   public final long rowCacheCapacity;
   public final long rowCacheEntries;
   public final long rowCacheSize;
   public final long commitLogSize;
   public final long commitLogPendingTasks;
   public final long completedMutations;
   public final long droppedMutations;
   public final double droppedMutationRatio;
   public final long backgroundIoPending;

   private NodeInfo(InetAddress address, String state, long uptime, Set<String> tokens, float ownership, String datacenter, String rack, long totalReads, long totalRangeSlices, long totalWrites, double meanReadLatency, double meanRangeSliceLatency, double meanWriteLatency, long readTimeouts, long rangeSliceTimeouts, long writeTimeouts, long heapMax, long heapUsed, long cmsCollectionCount, long cmsCollectionTime, long parnewCollectionCount, long parnewCollectionTime, long completedMutations, long droppedMutations, long compactionsCompleted, int compactionsPending, long readsPending, long mutationsPending, long readRepairPending, long repairPending, long gossipPending, long hintedHandoffPending, long internalResponsesPending, long migrationsPending, long miscTasksPending, long requestResponsesPending, long flushwritersPending, long memtablePostFlushPending, long replicateOnWritePending, int currentStreams, long totalDiskSpace, long freeDiskSpace, long totalTableDataSize, long totalIndexDataSize, long totalPhysicalMemory, double processCpuLoad, long totalBatchesReplayed, long keyCacheCapacity, long keyCacheEntries, long keyCacheSize, long rowCacheCapacity, long rowCacheEntries, long rowCacheSize, long commitLogSize, long commitLogPendingTasks, long backgroundIoPending) {
      this.address = address;
      this.state = state;
      this.uptime = uptime;
      this.tokens = tokens;
      this.ownership = ownership;
      this.datacenter = datacenter;
      this.rack = rack;
      this.totalReads = totalReads;
      this.totalRangeSlices = totalRangeSlices;
      this.totalWrites = totalWrites;
      this.meanReadLatency = meanReadLatency;
      this.meanRangeSliceLatency = meanRangeSliceLatency;
      this.meanWriteLatency = meanWriteLatency;
      this.readTimeouts = readTimeouts;
      this.rangeSliceTimeouts = rangeSliceTimeouts;
      this.writeTimeouts = writeTimeouts;
      this.heapMax = heapMax;
      this.heapUsed = heapUsed;
      this.cmsCollectionCount = cmsCollectionCount;
      this.cmsCollectionTime = cmsCollectionTime;
      this.parnewCollectionCount = parnewCollectionCount;
      this.parnewCollectionTime = parnewCollectionTime;
      this.completedMutations = completedMutations;
      this.droppedMutations = droppedMutations;
      this.compactionsCompleted = compactionsCompleted;
      this.compactionsPending = compactionsPending;
      this.readsPending = readsPending;
      this.mutationsPending = mutationsPending;
      this.readRepairPending = readRepairPending;
      this.repairPending = repairPending;
      this.gossipPending = gossipPending;
      this.hintedHandoffPending = hintedHandoffPending;
      this.internalResponsesPending = internalResponsesPending;
      this.migrationsPending = migrationsPending;
      this.miscTasksPending = miscTasksPending;
      this.requestResponsesPending = requestResponsesPending;
      this.flushwritersPending = flushwritersPending;
      this.memtablePostFlushPending = memtablePostFlushPending;
      this.replicateOnWritePending = replicateOnWritePending;
      this.currentStreams = currentStreams;
      this.totalDiskSpace = totalDiskSpace;
      this.freeDiskSpace = freeDiskSpace;
      this.totalTableDataSize = totalTableDataSize;
      this.totalIndexDataSize = totalIndexDataSize;
      this.totalPhysicalMemory = totalPhysicalMemory;
      this.processCpuLoad = processCpuLoad;
      this.totalBatchesReplayed = totalBatchesReplayed;
      this.keyCacheCapacity = keyCacheCapacity;
      this.keyCacheEntries = keyCacheEntries;
      this.keyCacheSize = keyCacheSize;
      this.rowCacheCapacity = rowCacheCapacity;
      this.rowCacheEntries = rowCacheEntries;
      this.rowCacheSize = rowCacheSize;
      this.commitLogSize = commitLogSize;
      this.commitLogPendingTasks = commitLogPendingTasks;
      this.droppedMutationRatio = -1.0D;
      this.backgroundIoPending = backgroundIoPending;
   }

   public List<ByteBuffer> toByteBufferList(CassandraVersion cassandraVersion) {
      List<ByteBuffer> vars = new LinkedList();
      vars.add(ByteBufferUtil.bytes(this.address));
      vars.add(ByteBufferUtil.bytes(this.state));
      vars.add(ByteBufferUtil.bytes(this.uptime));
      vars.add(SetType.getInstance(UTF8Type.instance, true).decompose(this.tokens));
      vars.add(ByteBufferUtil.bytes(this.ownership));
      vars.add(ByteBufferUtil.bytes(this.datacenter));
      vars.add(ByteBufferUtil.bytes(this.rack));
      vars.add(ByteBufferUtil.bytes(this.totalReads));
      vars.add(ByteBufferUtil.bytes(this.totalRangeSlices));
      vars.add(ByteBufferUtil.bytes(this.totalWrites));
      vars.add(ByteBufferUtil.bytes(this.meanReadLatency));
      vars.add(ByteBufferUtil.bytes(this.meanRangeSliceLatency));
      vars.add(ByteBufferUtil.bytes(this.meanWriteLatency));
      vars.add(ByteBufferUtil.bytes(this.readTimeouts));
      vars.add(ByteBufferUtil.bytes(this.rangeSliceTimeouts));
      vars.add(ByteBufferUtil.bytes(this.writeTimeouts));
      vars.add(ByteBufferUtil.bytes(this.heapMax));
      vars.add(ByteBufferUtil.bytes(this.heapUsed));
      vars.add(ByteBufferUtil.bytes(this.cmsCollectionCount));
      vars.add(ByteBufferUtil.bytes(this.cmsCollectionTime));
      vars.add(ByteBufferUtil.bytes(this.parnewCollectionCount));
      vars.add(ByteBufferUtil.bytes(this.parnewCollectionTime));
      vars.add(ByteBufferUtil.bytes(this.completedMutations));
      vars.add(ByteBufferUtil.bytes(this.droppedMutations));
      vars.add(ByteBufferUtil.bytes(this.droppedMutationRatio));
      vars.add(ByteBufferUtil.bytes(this.compactionsCompleted));
      vars.add(ByteBufferUtil.bytes(this.compactionsPending));
      vars.add(ByteBufferUtil.bytes(this.readsPending));
      vars.add(ByteBufferUtil.bytes(this.mutationsPending));
      vars.add(ByteBufferUtil.bytes(this.readRepairPending));
      vars.add(ByteBufferUtil.bytes(this.repairPending));
      vars.add(ByteBufferUtil.bytes(this.gossipPending));
      vars.add(ByteBufferUtil.bytes(this.hintedHandoffPending));
      vars.add(ByteBufferUtil.bytes(this.internalResponsesPending));
      vars.add(ByteBufferUtil.bytes(this.migrationsPending));
      vars.add(ByteBufferUtil.bytes(this.miscTasksPending));
      vars.add(ByteBufferUtil.bytes(this.requestResponsesPending));
      vars.add(ByteBufferUtil.bytes(this.flushwritersPending));
      vars.add(ByteBufferUtil.bytes(this.memtablePostFlushPending));
      vars.add(ByteBufferUtil.bytes(this.replicateOnWritePending));
      vars.add(ByteBufferUtil.bytes(this.currentStreams));
      vars.add(ByteBufferUtil.bytes(this.totalDiskSpace));
      vars.add(ByteBufferUtil.bytes(this.freeDiskSpace));
      vars.add(ByteBufferUtil.bytes(this.totalTableDataSize));
      vars.add(ByteBufferUtil.bytes(this.totalIndexDataSize));
      vars.add(ByteBufferUtil.bytes(this.totalPhysicalMemory));
      vars.add(ByteBufferUtil.bytes(this.processCpuLoad));
      vars.add(ByteBufferUtil.bytes(this.totalBatchesReplayed));
      vars.add(ByteBufferUtil.bytes(this.keyCacheCapacity));
      vars.add(ByteBufferUtil.bytes(this.keyCacheEntries));
      vars.add(ByteBufferUtil.bytes(this.keyCacheSize));
      vars.add(ByteBufferUtil.bytes(this.rowCacheCapacity));
      vars.add(ByteBufferUtil.bytes(this.rowCacheEntries));
      vars.add(ByteBufferUtil.bytes(this.rowCacheSize));
      vars.add(ByteBufferUtil.bytes(this.commitLogSize));
      vars.add(ByteBufferUtil.bytes(this.commitLogPendingTasks));
      if(cassandraVersion.compareTo(DseVersionNotifier.VERSION_60) >= 0) {
         vars.add(ByteBufferUtil.bytes(this.backgroundIoPending));
      }

      return vars;
   }

   public static NodeInfo fromRow(Row row) {
      return new NodeInfo(row.getInetAddress("node_ip"), row.getString("state"), row.getLong("uptime"), row.getSet("tokens", UTF8Type.instance), ByteBufferUtil.toFloat(row.getBytes("data_owned")), row.getString("datacenter"), row.getString("rack"), row.getLong("total_reads"), row.getLong("total_range_slices"), row.getLong("total_writes"), row.getDouble("mean_read_latency"), row.getDouble("mean_range_slice_latency"), row.getDouble("mean_write_latency"), row.getLong("read_timeouts"), row.getLong("range_slice_timeouts"), row.getLong("write_timeouts"), row.getLong("heap_total"), row.getLong("heap_used"), row.getLong("cms_collection_count"), row.getLong("cms_collection_time"), row.getLong("parnew_collection_count"), row.getLong("parnew_collection_time"), row.getLong("completed_mutations"), row.getLong("dropped_mutations"), row.getLong("compactions_completed"), row.getInt("compactions_pending"), row.getLong("read_requests_pending"), row.getLong("write_requests_pending"), row.getLong("read_repair_tasks_pending"), row.getLong("manual_repair_tasks_pending"), row.getLong("gossip_tasks_pending"), row.getLong("hinted_handoff_pending"), row.getLong("internal_responses_pending"), row.getLong("migrations_pending"), row.getLong("misc_tasks_pending"), row.getLong("request_responses_pending"), row.getLong("flush_sorter_tasks_pending"), row.getLong("memtable_post_flushers_pending"), row.getLong("replicate_on_write_tasks_pending"), row.getInt("streams_pending"), row.getLong("storage_capacity"), row.getLong("free_space"), row.getLong("table_data_size"), row.getLong("index_data_size"), row.getLong("total_node_memory"), row.getDouble("process_cpu_load"), row.getLong("total_batches_replayed"), row.getLong("key_cache_capacity"), row.getLong("key_cache_entries"), row.getLong("key_cache_size"), row.getLong("row_cache_capacity"), row.getLong("row_cache_entries"), row.getLong("row_cache_size"), row.getLong("commitlog_size"), row.getLong("commitlog_pending_tasks"), row.has("background_io_pending")?row.getLong("background_io_pending"):0L);
   }

   public static class Builder {
      private InetAddress localAddress;
      private IEndpointSnitch snitch;
      private BatchlogInfoProvider batchLog;
      private StreamInfoProvider streams;
      private StorageInfoProvider storage;
      private MessagingInfoProvider messaging;
      private CacheInfoProvider keyCache;
      private CacheInfoProvider rowCache;
      private ThreadPoolStatsProvider threadPoolStats;
      private ClientRequestMetricsProvider clientRequests;
      private CommitLogInfoProvider commitLog;
      private CompactionInfoProvider compaction;
      private SystemResourcesInfoProvider systemResources;
      private GCInfoProvider gc;
      private RuntimeMXBean runtime;
      private MemoryMXBean memory;

      public Builder() {
      }

      public NodeInfo build() {
         if(this.localAddress != null && this.storage != null && this.batchLog != null && this.streams != null && this.messaging != null && this.snitch != null && this.keyCache != null && this.rowCache != null && this.threadPoolStats != null && this.clientRequests != null && this.commitLog != null && this.compaction != null && this.systemResources != null && this.gc != null && this.runtime != null && this.memory != null) {
            Map<ThreadPoolStats.Pool, ThreadPoolStats> threadPools = this.threadPoolStats.getThreadPools();
            Map<String, Integer> droppedMessages = this.messaging.getDroppedMessages();
            return new NodeInfo(this.localAddress, this.storage.getState(), this.runtime.getUptime() / 1000L, this.storage.getTokens(), this.storage.getOwnership(), this.snitch.getDatacenter(this.localAddress), this.snitch.getRack(this.localAddress), this.clientRequests.totalReads(), this.clientRequests.totalRangeSlices(), this.clientRequests.totalWrites(), this.clientRequests.meanReadLatency(), this.clientRequests.meanRangeSliceLatency(), this.clientRequests.meanWriteLatency(), this.clientRequests.readTimeouts(), this.clientRequests.rangeSliceTimeouts(), this.clientRequests.writeTimeouts(), this.memory.getHeapMemoryUsage().getMax(), this.memory.getHeapMemoryUsage().getUsed(), this.gc.getCmsCollectionCount(), this.gc.getCmsCollectionTime(), this.gc.getParNewCollectionCount(), this.gc.getParNewCollectionTime(), -1L, ((Integer)droppedMessages.get(Group.MUTATION.toString())).longValue(), this.compaction.totalCompleted(), this.compaction.pendingTasks(), -1L, -1L, ((ThreadPoolStats)threadPools.get(ThreadPoolStats.Pool.READ_REPAIR)).getPendingTasks(), ((ThreadPoolStats)threadPools.get(ThreadPoolStats.Pool.ANTI_ENTROPY)).getPendingTasks(), ((ThreadPoolStats)threadPools.get(ThreadPoolStats.Pool.GOSSIP)).getPendingTasks(), ((ThreadPoolStats)threadPools.get(ThreadPoolStats.Pool.HINTED_HANDOFF)).getPendingTasks(), ((ThreadPoolStats)threadPools.get(ThreadPoolStats.Pool.INTERNAL_RESPONSE)).getPendingTasks(), ((ThreadPoolStats)threadPools.get(ThreadPoolStats.Pool.MIGRATION)).getPendingTasks(), ((ThreadPoolStats)threadPools.get(ThreadPoolStats.Pool.MISC)).getPendingTasks(), ((ThreadPoolStats)threadPools.get(ThreadPoolStats.Pool.REQUEST_RESPONSE)).getPendingTasks(), ((ThreadPoolStats)threadPools.get(ThreadPoolStats.Pool.MEMTABLE_FLUSH_WRITER)).getPendingTasks(), ((ThreadPoolStats)threadPools.get(ThreadPoolStats.Pool.MEMTABLE_POST_FLUSH)).getPendingTasks(), -1L, this.streams.getCurrentStreamsCount(), this.systemResources.getTotalPhysicalDiskSpace(), this.systemResources.getFreeDiskSpaceRemaining(), this.systemResources.getTotalTableDataSize(), this.systemResources.getTotalIndexDataSize(), this.systemResources.getTotalPhysicalMemory(), this.systemResources.getProcessCpuLoad(), this.batchLog.getTotalBatchesReplayed(), this.keyCache.getCapacity(), this.keyCache.getEntries(), this.keyCache.getSize(), this.rowCache.getCapacity(), this.rowCache.getEntries(), this.rowCache.getSize(), this.commitLog.getCommitLogTotalSize(), this.commitLog.getCommitLogPendingTasks(), ((ThreadPoolStats)threadPools.get(ThreadPoolStats.Pool.BACKGROUND_IO)).getPendingTasks());
         } else {
            throw new RuntimeException("NodeInfo.Builder not properly configured");
         }
      }

      public NodeInfo.Builder withLocalAddress(InetAddress address) {
         this.localAddress = address;
         return this;
      }

      public NodeInfo.Builder withStorageService(StorageInfoProvider storage) {
         this.storage = storage;
         return this;
      }

      public NodeInfo.Builder withBatchlogInfo(BatchlogInfoProvider batchLog) {
         this.batchLog = batchLog;
         return this;
      }

      public NodeInfo.Builder withMessaging(MessagingInfoProvider messaging) {
         this.messaging = messaging;
         return this;
      }

      public NodeInfo.Builder withStreams(StreamInfoProvider streams) {
         this.streams = streams;
         return this;
      }

      public NodeInfo.Builder withSnitch(IEndpointSnitch snitch) {
         this.snitch = snitch;
         return this;
      }

      public NodeInfo.Builder withKeyCache(CacheInfoProvider keyCache) {
         this.keyCache = keyCache;
         return this;
      }

      public NodeInfo.Builder withRowCache(CacheInfoProvider rowCache) {
         this.rowCache = rowCache;
         return this;
      }

      public NodeInfo.Builder withThreadPools(ThreadPoolStatsProvider threadPoolStats) {
         this.threadPoolStats = threadPoolStats;
         return this;
      }

      public NodeInfo.Builder withClientRequests(ClientRequestMetricsProvider clientRequests) {
         this.clientRequests = clientRequests;
         return this;
      }

      public NodeInfo.Builder withCommitLog(CommitLogInfoProvider commitLog) {
         this.commitLog = commitLog;
         return this;
      }

      public NodeInfo.Builder withCompaction(CompactionInfoProvider compaction) {
         this.compaction = compaction;
         return this;
      }

      public NodeInfo.Builder withSystemResources(SystemResourcesInfoProvider systemResources) {
         this.systemResources = systemResources;
         return this;
      }

      public NodeInfo.Builder withGcInfo(GCInfoProvider gc) {
         this.gc = gc;
         return this;
      }

      public NodeInfo.Builder withRuntime(RuntimeMXBean runtime) {
         this.runtime = runtime;
         return this;
      }

      public NodeInfo.Builder withMemory(MemoryMXBean memory) {
         this.memory = memory;
         return this;
      }
   }
}
