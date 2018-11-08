package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ObjectSizes;

public final class TrieIndexEntry extends RowIndexEntry {
   private static final long BASE_SIZE;
   final long indexTrieRoot;
   private final int rowIndexCount;
   private final DeletionTime deletionTime;

   TrieIndexEntry(long dataFilePosition, long indexTrieRoot, int rowIndexCount, DeletionTime deletionTime) {
      super(dataFilePosition);

      assert rowIndexCount > 1;

      this.indexTrieRoot = indexTrieRoot;
      this.rowIndexCount = rowIndexCount;
      this.deletionTime = deletionTime;
   }

   public int rowIndexCount() {
      return this.rowIndexCount;
   }

   public DeletionTime deletionTime() {
      return this.deletionTime;
   }

   public long unsharedHeapSize() {
      return BASE_SIZE;
   }

   public void serialize(DataOutputPlus indexFile, long basePosition) throws IOException {
      indexFile.writeUnsignedVInt(this.position);
      indexFile.writeVInt(this.indexTrieRoot - basePosition);
      indexFile.writeUnsignedVInt((long)this.rowIndexCount);
      DeletionTime.serializer.serialize(this.deletionTime, indexFile);
   }

   public static RowIndexEntry create(long dataStartPosition, long trieRoot, DeletionTime partitionLevelDeletion, int rowIndexCount) {
      return (RowIndexEntry)(trieRoot == -1L?new RowIndexEntry(dataStartPosition):new TrieIndexEntry(dataStartPosition, trieRoot, rowIndexCount, partitionLevelDeletion));
   }

   public static RowIndexEntry deserialize(DataInputPlus in, long basePosition) throws IOException {
      long dataFilePosition = in.readUnsignedVInt();
      long indexTrieRoot = in.readVInt() + basePosition;
      int rowIndexCount = (int)in.readUnsignedVInt();
      DeletionTime deletionTime = DeletionTime.serializer.deserialize(in);
      return new TrieIndexEntry(dataFilePosition, indexTrieRoot, rowIndexCount, deletionTime);
   }

   static {
      BASE_SIZE = ObjectSizes.measure(new TrieIndexEntry(0L, 0L, 10, DeletionTime.LIVE));
   }
}
