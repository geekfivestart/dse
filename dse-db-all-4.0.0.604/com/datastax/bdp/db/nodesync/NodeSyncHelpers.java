package com.datastax.bdp.db.nodesync;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.SystemTimeSource;
import org.apache.cassandra.utils.TimeSource;
import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.Units;

abstract class NodeSyncHelpers {
   static final long NO_VALIDATION_TIME = -9223372036854775808L;
   private static ToLongFunction<ColumnFamilyStore> tableSizeProvider = NodeSyncHelpers::defaultTableSizeProvider;
   private static Function<String, Collection<Range<Token>>> localRangesProvider = NodeSyncHelpers::defaultLocalRangeProvider;
   private static final long DEFAULT_SEGMENT_SIZE_TARGET;
   private static long segmentSizeTarget;
   private static TimeSource timeSource;

   private NodeSyncHelpers() {
   }

   @VisibleForTesting
   static void setTestParameters(ToLongFunction<ColumnFamilyStore> sizeProvider, Function<String, Collection<Range<Token>>> rangeProvider, long segmentSize, TimeSource time) {
      if(sizeProvider != null) {
         tableSizeProvider = sizeProvider;
      }

      if(rangeProvider != null) {
         localRangesProvider = rangeProvider;
      }

      if(segmentSize >= 0L) {
         segmentSizeTarget = segmentSize;
      }

      if(time != null) {
         timeSource = time;
      }

   }

   @VisibleForTesting
   static void resetTestParameters() {
      tableSizeProvider = NodeSyncHelpers::defaultTableSizeProvider;
      localRangesProvider = NodeSyncHelpers::defaultLocalRangeProvider;
      segmentSizeTarget = DEFAULT_SEGMENT_SIZE_TARGET;
      timeSource = new SystemTimeSource();
   }

   private static Collection<Range<Token>> defaultLocalRangeProvider(String str) {
      return StorageService.instance.getLocalRanges(str);
   }

   private static long defaultTableSizeProvider(ColumnFamilyStore t) {
      long onDiskDataSize = 0L;

      SSTableReader sstable;
      for(Iterator var3 = t.getSSTables(SSTableSet.CANONICAL).iterator(); var3.hasNext(); onDiskDataSize += sstable.uncompressedLength()) {
         sstable = (SSTableReader)var3.next();
      }

      return t.getMemtablesLiveSize() + onDiskDataSize;
   }

   static long segmentSizeTarget() {
      return segmentSizeTarget;
   }

   static TimeSource time() {
      return timeSource;
   }

   static long estimatedSizeOf(ColumnFamilyStore table) {
      return tableSizeProvider.applyAsLong(table);
   }

   static Collection<Range<Token>> localRanges(String keyspace) {
      return (Collection)localRangesProvider.apply(keyspace);
   }

   static Stream<TableMetadata> nodeSyncEnabledTables() {
      return nodeSyncEnabledStores().map(ColumnFamilyStore::metadata);
   }

   static Stream<ColumnFamilyStore> nodeSyncEnabledStores() {
      return StorageService.instance.getNonSystemKeyspaces().stream().map(Keyspace::open).flatMap(NodeSyncHelpers::nodeSyncEnabledStores);
   }

   static Stream<TableMetadata> nodeSyncEnabledTables(Keyspace keyspace) {
      return nodeSyncEnabledStores(keyspace).map(ColumnFamilyStore::metadata);
   }

   static Stream<ColumnFamilyStore> nodeSyncEnabledStores(Keyspace keyspace) {
      return !isReplicated(keyspace)?Stream.empty():keyspace.getColumnFamilyStores().stream().filter((s) -> {
         return s.metadata().params.nodeSync.isEnabled(s.metadata());
      });
   }

   static boolean isReplicated(Keyspace keyspace) {
      return keyspace.getReplicationStrategy().getReplicationFactor() > 1 && !localRanges(keyspace.getName()).isEmpty();
   }

   static boolean isNodeSyncEnabled(ColumnFamilyStore store) {
      return StorageService.instance.getTokenMetadata().getAllEndpoints().size() > 1 && isReplicated(store.keyspace) && store.metadata().params.nodeSync.isEnabled(store.metadata());
   }

   static String sinceStr(long validationTimeMs) {
      if(validationTimeMs < 0L) {
         return "<no validation recorded>";
      } else {
         long now = time().currentTimeMillis();
         return String.format("%s ago", new Object[]{Units.toString(now - validationTimeMs, TimeUnit.MILLISECONDS)});
      }
   }

   static {
      DEFAULT_SEGMENT_SIZE_TARGET = Long.getLong("dse.nodesync.segment_size_target_bytes", SizeUnit.MEGABYTES.toBytes(200L)).longValue();
      segmentSizeTarget = DEFAULT_SEGMENT_SIZE_TARGET;
      timeSource = new SystemTimeSource();
   }
}
