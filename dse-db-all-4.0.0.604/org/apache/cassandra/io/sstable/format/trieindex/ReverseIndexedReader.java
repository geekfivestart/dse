package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;

class ReverseIndexedReader extends ReverseReader {
   private RowIndexReverseIterator indexReader;
   final TrieIndexEntry indexEntry;
   final FileHandle rowIndexFile;
   long basePosition;
   long currentBlockStart;
   long currentBlockEnd;
   RowIndexReader.IndexInfo currentIndexInfo;
   Rebufferer.ReaderConstraint rc;

   public ReverseIndexedReader(TrieIndexSSTableReader sstable, TrieIndexEntry indexEntry, Slices slices, FileDataInput file, boolean shouldCloseFile, SerializationHelper helper, Rebufferer.ReaderConstraint rc) {
      super(sstable, slices, file, shouldCloseFile, helper);
      this.basePosition = indexEntry.position;
      this.indexEntry = indexEntry;
      this.rc = rc;
      this.rowIndexFile = sstable.rowIndexFile;
   }

   public void close() throws IOException {
      try {
         this.indexReader.close();
      } finally {
         super.close();
      }

   }

   public boolean setForSlice(Slice slice) throws IOException {
      if(this.currentIndexInfo == null) {
         this.start = slice.start();
         this.end = slice.end();
         this.foundLessThan = false;
         ClusteringComparator comparator = this.metadata.comparator;
         if(this.indexReader != null) {
            this.indexReader.close();
            this.indexReader = null;
         }

         this.indexReader = new RowIndexReverseIterator(this.rowIndexFile, this.indexEntry, comparator.asByteComparableSource(this.end), this.rc);
         this.sliceOpenMarker = null;
         this.currentIndexInfo = this.indexReader.nextIndexInfo();
         if(this.currentIndexInfo == null) {
            return false;
         }
      }

      return this.gotoIndexBlock();
   }

   boolean gotoIndexBlock() throws IOException {
      assert this.rowOffsets.isEmpty();

      this.openMarker = this.currentIndexInfo.openDeletion;
      this.currentBlockStart = this.basePosition + this.currentIndexInfo.offset;
      this.seekToPosition(this.currentBlockStart);
      this.currentIndexInfo = null;
      return true;
   }

   protected boolean advanceBlock() throws IOException {
      if(this.foundLessThan) {
         return false;
      } else {
         if(this.currentIndexInfo == null) {
            this.currentBlockEnd = this.currentBlockStart;
            this.currentIndexInfo = this.indexReader.nextIndexInfo();
            if(this.currentIndexInfo == null) {
               return false;
            }
         }

         return this.gotoIndexBlock();
      }
   }

   protected boolean preBlockStep() throws IOException {
      return this.filePos >= this.currentBlockEnd || this.preStep(this.start);
   }

   protected boolean blockPrepStep() throws IOException {
      return this.filePos >= this.currentBlockEnd || this.prepStep(ClusteringBound.TOP);
   }
}
