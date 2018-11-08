package org.apache.cassandra.io.sstable.format.trieindex;

import com.carrotsearch.hppc.LongStack;
import java.io.IOException;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.sstable.format.AbstractReader;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;

class ReverseReader extends AbstractReader {
   LongStack rowOffsets = new LongStack();
   DeletionTime sliceOpenMarker;
   DeletionTime sliceCloseMarker;
   boolean foundLessThan;
   long startPos = -1L;

   ReverseReader(SSTableReader sstable, Slices slices, FileDataInput file, boolean shouldCloseFile, SerializationHelper helper) {
      super(sstable, slices, file, shouldCloseFile, helper, true);
   }

   public boolean setForSlice(Slice slice) throws IOException {
      if(this.startPos == -1L) {
         this.startPos = this.file.getFilePointer();
      } else {
         this.seekToPosition(this.startPos);
      }

      assert this.rowOffsets.isEmpty();

      return super.setForSlice(slice);
   }

   protected RangeTombstoneMarker sliceStartMarker() {
      return markerFrom(this.end, this.sliceCloseMarker);
   }

   protected Unfiltered nextInSlice() throws IOException {
      while(true) {
         if(!this.rowOffsets.isEmpty()) {
            this.seekToPosition(this.rowOffsets.peek());
            boolean hasNext = this.deserializer.hasNext();

            assert hasNext;

            Unfiltered toReturn = this.deserializer.readNext();
            this.rowOffsets.pop();
            if(toReturn.isEmpty()) {
               continue;
            }

            return toReturn;
         }

         return null;
      }
   }

   protected RangeTombstoneMarker sliceEndMarker() {
      return markerFrom(this.start, this.sliceOpenMarker);
   }

   protected boolean preSliceStep() throws IOException {
      return this.preStep(this.start);
   }

   protected boolean slicePrepStep() throws IOException {
      return this.prepStep(this.end);
   }

   boolean preStep(ClusteringBound start) throws IOException {
      assert this.filePos == this.file.getFilePointer();

      if(this.skipSmallerRow(start)) {
         this.sliceOpenMarker = this.openMarker;
         return true;
      } else {
         this.foundLessThan = true;
         return false;
      }
   }

   boolean prepStep(ClusteringBound end) throws IOException {
      if(this.skipSmallerRow(end)) {
         this.sliceCloseMarker = this.openMarker;
         return true;
      } else {
         this.rowOffsets.push(this.filePos);
         return false;
      }
   }
}
