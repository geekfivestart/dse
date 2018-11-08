package org.apache.cassandra.io.sstable.format;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.utils.ByteBufferUtil;

public class IndexFileEntry {
   public static final IndexFileEntry EMPTY = new IndexFileEntry();
   public final DecoratedKey key;
   public final RowIndexEntry entry;

   public IndexFileEntry() {
      this((DecoratedKey)null, (RowIndexEntry)null);
   }

   public IndexFileEntry(DecoratedKey key, RowIndexEntry entry) {
      this.key = key;
      this.entry = entry;
   }

   public String toString() {
      return String.format("[key: %s, indexed: %s, rows: %d]", new Object[]{this.key == null?"null":new String(ByteBufferUtil.getArrayUnsafe(this.key.getKey())), Boolean.valueOf(this.entry == null?false:this.entry.isIndexed()), Integer.valueOf(this.entry == null?0:this.entry.rowIndexCount())});
   }
}
