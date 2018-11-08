package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.Rebufferer;

class ReverseIndexedReader extends ReverseReader {
   private final IndexState indexState;
   private int lastBlockIdx;

   ReverseIndexedReader(BigTableReader sstable, BigRowIndexEntry indexEntry, Slices slices, FileDataInput file, boolean shouldCloseFile, SerializationHelper helper, Rebufferer.ReaderConstraint rc) {
      super(sstable, slices, file, shouldCloseFile, helper);
      this.indexState = new IndexState(this, this.metadata.comparator, indexEntry, true, sstable.ifile, rc);
   }

   public void close() throws IOException {
      try {
         super.close();
      } finally {
         this.indexState.close();
      }

   }

   public void resetReaderState() throws IOException {
      super.resetReaderState();
      this.indexState.reset();
   }

   public boolean setForSlice(Slice slice) throws IOException {
      super.setForSlice(slice);
      if(this.indexState.isDone()) {
         return false;
      } else {
         int startIdx = this.indexState.findBlockIndex(slice.end(), this.indexState.currentBlockIdx());
         if(startIdx < 0) {
            return false;
         } else {
            this.lastBlockIdx = this.indexState.findBlockIndex(slice.start(), startIdx);
            if(this.lastBlockIdx >= this.indexState.blocksCount()) {
               assert startIdx >= this.indexState.blocksCount();

               return false;
            } else {
               if(startIdx >= this.indexState.blocksCount()) {
                  startIdx = this.indexState.blocksCount() - 1;
               }

               this.indexState.setToBlock(startIdx);
               this.prepStage = ReverseReader.PrepStage.OPEN;
               return true;
            }
         }
      }
   }

   protected boolean preSliceStep() throws IOException {
      return this.indexState.currentBlockIdx() != this.lastBlockIdx?true:this.skipSmallerRow(this.start);
   }

   protected boolean slicePrepStep() throws IOException {
      if(this.buffer == null) {
         this.buffer = this.createBuffer(this.indexState.blocksCount());
      }

      return this.prepStep(this.indexState.currentBlockIdx() != this.lastBlockIdx, false);
   }

   protected boolean preBlockStep() throws IOException {
      return this.indexState.currentBlockIdx() != this.lastBlockIdx?true:this.skipSmallerRow(this.start);
   }

   protected boolean blockPrepStep() throws IOException {
      return this.prepStep(this.indexState.currentBlockIdx() != this.lastBlockIdx, true);
   }

   public boolean advanceBlock() throws IOException {
      int nextBlockIdx = this.indexState.currentBlockIdx() - 1;
      if(nextBlockIdx >= 0 && nextBlockIdx >= this.lastBlockIdx) {
         this.indexState.setToBlock(nextBlockIdx);
         this.prepStage = ReverseReader.PrepStage.OPEN;
         return true;
      } else {
         return false;
      }
   }

   protected boolean stopReadingDisk() throws IOException {
      return this.indexState.isPastCurrentBlock();
   }
}
