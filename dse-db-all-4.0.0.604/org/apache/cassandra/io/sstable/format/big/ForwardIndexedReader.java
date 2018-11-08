package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.Rebufferer;

class ForwardIndexedReader extends ForwardReader {
   private final IndexState indexState;
   private int lastBlockIdx;

   ForwardIndexedReader(SSTableReader sstable, BigRowIndexEntry indexEntry, Slices slices, FileDataInput file, boolean shouldCloseFile, SerializationHelper helper, Rebufferer.ReaderConstraint rc) {
      super(sstable, slices, file, shouldCloseFile, helper);
      this.indexState = new IndexState(this, this.metadata.comparator, indexEntry, false, ((BigTableReader)sstable).ifile, rc);
      this.lastBlockIdx = this.indexState.blocksCount();
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
         int startIdx = this.indexState.findBlockIndex(slice.start(), this.indexState.currentBlockIdx());
         if(startIdx >= this.indexState.blocksCount()) {
            return false;
         } else {
            this.lastBlockIdx = this.indexState.findBlockIndex(slice.end(), startIdx);
            if(this.lastBlockIdx < 0) {
               assert startIdx < 0;

               return false;
            } else {
               if(startIdx < 0) {
                  startIdx = 0;
               }

               if(startIdx != this.indexState.currentBlockIdx()) {
                  this.indexState.setToBlock(startIdx);
               }

               return this.indexState.currentBlockIdx() != this.lastBlockIdx || this.metadata.comparator.compare((ClusteringPrefix)slice.end(), (ClusteringPrefix)this.indexState.currentIndex().firstName) >= 0 || this.openMarker != null;
            }
         }
      }
   }

   protected Unfiltered nextInSlice() throws IOException {
      Unfiltered next;
      do {
         this.indexState.updateBlock();
         if(this.indexState.isDone() || this.indexState.currentBlockIdx() > this.lastBlockIdx || !this.deserializer.hasNext() || this.indexState.currentBlockIdx() == this.lastBlockIdx && this.deserializer.compareNextTo(this.end) >= 0) {
            return null;
         }

         next = this.deserializer.readNext();
      } while(next.isEmpty());

      if(next.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER) {
         this.updateOpenMarker((RangeTombstoneMarker)next);
      }

      return next;
   }
}
