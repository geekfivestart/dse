package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.Rebufferer;

class ForwardIndexedReader extends ForwardReader {
   private final RowIndexReader indexReader;
   long basePosition;

   ForwardIndexedReader(TrieIndexSSTableReader sstable, TrieIndexEntry indexEntry, Slices slices, FileDataInput file, boolean shouldCloseFile, SerializationHelper helper, Rebufferer.ReaderConstraint rc) {
      super(sstable, slices, file, shouldCloseFile, helper);
      this.basePosition = indexEntry.position;
      this.indexReader = new RowIndexReader(sstable.rowIndexFile, indexEntry, rc);
   }

   public void close() throws IOException {
      try {
         this.indexReader.close();
      } finally {
         super.close();
      }

   }

   public boolean setForSlice(Slice slice) throws IOException {
      super.setForSlice(slice);
      RowIndexReader.IndexInfo indexInfo = this.indexReader.separatorFloor(this.metadata.comparator.asByteComparableSource(slice.start()));

      assert indexInfo != null;

      long position = this.basePosition + indexInfo.offset;
      if(this.filePos == -1L || this.filePos < position) {
         this.openMarker = indexInfo.openDeletion;
         this.seekToPosition(position);
      }

      return true;
   }
}
