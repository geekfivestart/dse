package com.datastax.bdp.reporting.snapshots.node;

import com.datastax.bdp.reporting.CqlWritable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;

public class DatacenterInfo implements Iterable<NodeInfo>, CqlWritable {
   public final String name;
   private final List<NodeInfo> nodes = new ArrayList();
   private AggregateNodeInfo summary;

   public DatacenterInfo(String name) {
      this.name = name;
   }

   public void addNode(NodeInfo node) {
      this.nodes.add(node);
   }

   public Iterator<NodeInfo> iterator() {
      return this.nodes.iterator();
   }

   public AggregateNodeInfo getSummaryStats() {
      if(this.summary == null) {
         this.summary = this.summarize();
      }

      return this.summary;
   }

   public List<ByteBuffer> toByteBufferList(CassandraVersion cassandraVersion) {
      List<ByteBuffer> vars = this.getSummaryStats().toByteBufferList(cassandraVersion);
      vars.add(0, ByteBufferUtil.bytes(this.name));
      return vars;
   }

   private AggregateNodeInfo summarize() {
      AggregateNodeInfo summary = new AggregateNodeInfo();

      NodeInfo node;
      for(Iterator var2 = this.nodes.iterator(); var2.hasNext(); summary.backgroundIoPending += node.backgroundIoPending) {
         node = (NodeInfo)var2.next();
         summary.meanReadLatency = AggregateNodeInfo.addMeans(summary.meanReadLatency, summary.totalReads, node.meanReadLatency, node.totalReads);
         summary.meanRangeSliceLatency = AggregateNodeInfo.addMeans(summary.meanRangeSliceLatency, summary.totalRangeSlices, node.meanRangeSliceLatency, node.totalRangeSlices);
         summary.meanWriteLatency = AggregateNodeInfo.addMeans(summary.meanWriteLatency, summary.totalWrites, node.meanWriteLatency, node.totalWrites);
         ++summary.nodeCount;
         summary.totalReads += node.totalReads;
         summary.totalRangeSlices += node.totalRangeSlices;
         summary.totalWrites += node.totalWrites;
         summary.readsPending = -1L;
         summary.mutationsPending = -1L;
         summary.readRepairPending += node.readRepairPending;
         summary.repairPending += node.repairPending;
         summary.gossipPending += node.gossipPending;
         summary.hintedHandoffPending += node.hintedHandoffPending;
         summary.internalResponsePending += node.internalResponsesPending;
         summary.migrationsPending += node.migrationsPending;
         summary.miscTasksPending += node.miscTasksPending;
         summary.requestResponsePending += node.requestResponsesPending;
         summary.flushwriterPending += node.flushwritersPending;
         summary.memtablePostFlushPending += node.memtablePostFlushPending;
         summary.replicateOnWritePending = -1L;
         summary.streamPending += node.currentStreams;
         summary.storageCapacity += node.totalDiskSpace;
         summary.freeSpace += node.freeDiskSpace;
         summary.tableData += node.totalTableDataSize;
         summary.indexData += node.totalIndexDataSize;
         summary.compactionsComplete += node.compactionsCompleted;
         summary.compactionsPending += node.compactionsPending;
         summary.batchesReplayed += node.totalBatchesReplayed;
         summary.keyCacheEntries += node.keyCacheEntries;
         summary.keyCacheSize += node.keyCacheSize;
         summary.keyCacheCapacity += node.keyCacheCapacity;
         summary.rowCacheEntries += node.rowCacheEntries;
         summary.rowCacheSize += node.rowCacheSize;
         summary.rowCacheCapacity += node.rowCacheCapacity;
         summary.completedMutations = -1L;
         summary.droppedMutations += node.droppedMutations;
         summary.droppedMutationRatio = -1.0D;
      }

      return summary;
   }
}
