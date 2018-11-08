package org.apache.cassandra.io.sstable.format;

import org.apache.cassandra.io.sstable.RowIndexEntry;

public interface SSTableReadsListener {
   SSTableReadsListener NOOP_LISTENER = new SSTableReadsListener() {
   };

   default void onSSTableSkipped(SSTableReader sstable, SSTableReadsListener.SkippingReason reason) {
   }

   default void onSSTableSelected(SSTableReader sstable, RowIndexEntry indexEntry, SSTableReadsListener.SelectionReason reason) {
   }

   default void onScanningStarted(SSTableReader sstable) {
   }

   public static enum SelectionReason {
      KEY_CACHE_HIT,
      INDEX_ENTRY_FOUND;

      private SelectionReason() {
      }
   }

   public static enum SkippingReason {
      BLOOM_FILTER,
      MIN_MAX_KEYS,
      PARTITION_INDEX_LOOKUP,
      INDEX_ENTRY_NOT_FOUND;

      private SkippingReason() {
      }
   }
}
