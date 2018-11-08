package com.datastax.bdp.reporting.snapshots.node;

import com.datastax.bdp.gms.DseVersionNotifier;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;

public class AggregateNodeInfo {
   int nodeCount = 0;
   long totalReads = 0L;
   long totalRangeSlices = 0L;
   long totalWrites = 0L;
   double meanReadLatency = 0.0D;
   double meanRangeSliceLatency = 0.0D;
   double meanWriteLatency = 0.0D;
   long readsPending = -1L;
   long mutationsPending = -1L;
   long readRepairPending = 0L;
   long repairPending = 0L;
   long gossipPending = 0L;
   long hintedHandoffPending = 0L;
   long internalResponsePending = 0L;
   long migrationsPending = 0L;
   long miscTasksPending = 0L;
   long requestResponsePending = 0L;
   long flushwriterPending = 0L;
   long memtablePostFlushPending = 0L;
   long replicateOnWritePending = -1L;
   int streamPending = 0;
   long storageCapacity = 0L;
   long freeSpace = 0L;
   long tableData = 0L;
   long indexData = 0L;
   long compactionsComplete = 0L;
   int compactionsPending = 0;
   long batchesReplayed = 0L;
   long keyCacheEntries = 0L;
   long keyCacheSize = 0L;
   long keyCacheCapacity = 0L;
   long rowCacheEntries = 0L;
   long rowCacheSize = 0L;
   long rowCacheCapacity = 0L;
   long completedMutations = -1L;
   long droppedMutations = 0L;
   double droppedMutationRatio = -1.0D;
   long backgroundIoPending = 0L;

   public AggregateNodeInfo() {
   }

   public void add(AggregateNodeInfo other) {
      this.meanReadLatency = addMeans(this.meanWriteLatency, this.totalReads, other.meanReadLatency, other.totalReads);
      this.meanRangeSliceLatency = addMeans(this.meanRangeSliceLatency, this.totalRangeSlices, other.meanRangeSliceLatency, other.totalRangeSlices);
      this.meanWriteLatency = addMeans(this.meanWriteLatency, this.totalWrites, other.meanWriteLatency, other.totalWrites);
      this.nodeCount += other.nodeCount;
      this.totalReads += other.totalReads;
      this.totalRangeSlices += other.totalRangeSlices;
      this.totalWrites += other.totalWrites;
      this.readsPending = -1L;
      this.mutationsPending = -1L;
      this.readRepairPending += other.readRepairPending;
      this.repairPending += other.repairPending;
      this.gossipPending += other.gossipPending;
      this.hintedHandoffPending += other.hintedHandoffPending;
      this.internalResponsePending += other.internalResponsePending;
      this.migrationsPending += other.migrationsPending;
      this.miscTasksPending += other.miscTasksPending;
      this.requestResponsePending += other.requestResponsePending;
      this.flushwriterPending += other.flushwriterPending;
      this.memtablePostFlushPending += other.memtablePostFlushPending;
      this.replicateOnWritePending = -1L;
      this.streamPending += other.streamPending;
      this.storageCapacity += other.storageCapacity;
      this.freeSpace += other.freeSpace;
      this.tableData += other.tableData;
      this.indexData += other.indexData;
      this.compactionsComplete += other.compactionsComplete;
      this.compactionsPending += other.compactionsPending;
      this.batchesReplayed += other.batchesReplayed;
      this.keyCacheEntries += other.keyCacheEntries;
      this.keyCacheSize += other.keyCacheSize;
      this.keyCacheCapacity += other.keyCacheCapacity;
      this.rowCacheEntries += other.rowCacheEntries;
      this.rowCacheSize += other.rowCacheSize;
      this.rowCacheCapacity += other.rowCacheCapacity;
      this.completedMutations = -1L;
      this.droppedMutations += other.droppedMutations;
      this.droppedMutationRatio = -1.0D;
      this.backgroundIoPending += other.backgroundIoPending;
   }

   public List<ByteBuffer> toByteBufferList(CassandraVersion cassandraVersion) {
      List<ByteBuffer> vars = new LinkedList();
      vars.add(ByteBufferUtil.bytes(this.nodeCount));
      vars.add(ByteBufferUtil.bytes(this.totalReads));
      vars.add(ByteBufferUtil.bytes(this.totalRangeSlices));
      vars.add(ByteBufferUtil.bytes(this.totalWrites));
      vars.add(ByteBufferUtil.bytes(this.meanReadLatency));
      vars.add(ByteBufferUtil.bytes(this.meanRangeSliceLatency));
      vars.add(ByteBufferUtil.bytes(this.meanWriteLatency));
      vars.add(ByteBufferUtil.bytes(this.completedMutations));
      vars.add(ByteBufferUtil.bytes(this.droppedMutations));
      vars.add(ByteBufferUtil.bytes(this.droppedMutationRatio));
      vars.add(ByteBufferUtil.bytes(this.storageCapacity));
      vars.add(ByteBufferUtil.bytes(this.freeSpace));
      vars.add(ByteBufferUtil.bytes(this.tableData));
      vars.add(ByteBufferUtil.bytes(this.indexData));
      vars.add(ByteBufferUtil.bytes(this.compactionsComplete));
      vars.add(ByteBufferUtil.bytes(this.compactionsPending));
      vars.add(ByteBufferUtil.bytes(this.readsPending));
      vars.add(ByteBufferUtil.bytes(this.mutationsPending));
      vars.add(ByteBufferUtil.bytes(this.readRepairPending));
      vars.add(ByteBufferUtil.bytes(this.repairPending));
      vars.add(ByteBufferUtil.bytes(this.gossipPending));
      vars.add(ByteBufferUtil.bytes(this.hintedHandoffPending));
      vars.add(ByteBufferUtil.bytes(this.internalResponsePending));
      vars.add(ByteBufferUtil.bytes(this.migrationsPending));
      vars.add(ByteBufferUtil.bytes(this.miscTasksPending));
      vars.add(ByteBufferUtil.bytes(this.requestResponsePending));
      vars.add(ByteBufferUtil.bytes(this.flushwriterPending));
      vars.add(ByteBufferUtil.bytes(this.memtablePostFlushPending));
      vars.add(ByteBufferUtil.bytes(this.replicateOnWritePending));
      vars.add(ByteBufferUtil.bytes(this.streamPending));
      vars.add(ByteBufferUtil.bytes(this.batchesReplayed));
      vars.add(ByteBufferUtil.bytes(this.keyCacheEntries));
      vars.add(ByteBufferUtil.bytes(this.keyCacheSize));
      vars.add(ByteBufferUtil.bytes(this.keyCacheCapacity));
      vars.add(ByteBufferUtil.bytes(this.rowCacheEntries));
      vars.add(ByteBufferUtil.bytes(this.rowCacheSize));
      vars.add(ByteBufferUtil.bytes(this.rowCacheCapacity));
      if(cassandraVersion.compareTo(DseVersionNotifier.VERSION_60) >= 0) {
         vars.add(ByteBufferUtil.bytes(this.backgroundIoPending));
      }

      return vars;
   }

   public static double addMeans(double mean1, long total1, double mean2, long total2) {
      return total1 + total2 == 0L?0.0D:(mean1 * (double)total1 + mean2 * (double)total2) / (double)(total1 + total2);
   }
}
