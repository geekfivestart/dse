package org.apache.cassandra.io.sstable.format;

import java.io.IOException;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.UnfilteredDeserializer;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.schema.TableMetadata;

public abstract class AbstractReader implements SSTableReader.PartitionReader {
   protected final TableMetadata metadata;
   protected final Slices slices;
   private final boolean shouldCloseFile;
   public FileDataInput file;
   public UnfilteredDeserializer deserializer;
   protected ClusteringBound start;
   protected ClusteringBound end;
   private Slice pendingSlice;
   public DeletionTime openMarker;
   protected long filePos;
   AbstractReader.Stage stage;
   private final int direction;
   private int currentSlice;

   protected AbstractReader(SSTableReader sstable, Slices slices, FileDataInput file, boolean shouldCloseFile, SerializationHelper helper, boolean reversed) {
      this.start = ClusteringBound.BOTTOM;
      this.end = ClusteringBound.TOP;
      this.openMarker = null;
      this.filePos = -1L;
      this.stage = AbstractReader.Stage.NEEDS_SLICE;

      assert file != null;

      this.metadata = sstable.metadata();
      this.slices = slices;
      this.file = file;
      this.shouldCloseFile = shouldCloseFile;
      this.direction = reversed?-1:1;
      this.deserializer = UnfilteredDeserializer.create(this.metadata, file, sstable.header, helper);
      this.currentSlice = reversed?slices.size():-1;
   }

   public Unfiltered next() throws IOException {
      while (true) {
         Label_0311: {
            switch (this.stage) {
               case NEEDS_SLICE: {
                  this.currentSlice += this.direction;
                  if (this.currentSlice < 0 || this.currentSlice >= this.slices.size()) {
                     return null;
                  }
                  assert this.pendingSlice == null;
                  this.pendingSlice = this.slices.get(this.currentSlice);
                  this.stage = Stage.NEEDS_SET_FOR_SLICE;
               }
               case NEEDS_SET_FOR_SLICE: {
                  this.filePos = -1L;
                  if (!this.setForSlice(this.pendingSlice)) {
                     this.pendingSlice = null;
                     this.stage = Stage.NEEDS_SLICE;
                     continue;
                  }
                  this.pendingSlice = null;
                  this.stage = Stage.NEEDS_PRE_SLICE;
                  break Label_0311;
               }
               case NEEDS_PRE_SLICE: {
                  do {
                     this.filePos = this.file.getFilePointer();
                  } while (!this.preSliceStep());
                  this.stage = Stage.NEEDS_SLICE_PREP;
               }
               case NEEDS_SLICE_PREP: {
                  while (!this.slicePrepStep()) {
                     this.filePos = this.file.getFilePointer();
                  }
                  this.stage = Stage.READY;
                  final Unfiltered next = this.sliceStartMarker();
                  if (next != null) {
                     return next;
                  }
               }
               case READY: {
                  final Unfiltered next = this.nextInSlice();
                  this.filePos = this.file.getFilePointer();
                  if (next != null) {
                     return next;
                  }
                  this.stage = Stage.NEEDS_BLOCK;
               }
               case NEEDS_BLOCK: {
                  if (this.advanceBlock()) {
                     this.stage = Stage.NEEDS_PRE_BLOCK;
                     break Label_0311;
                  }
                  this.stage = Stage.NEEDS_SLICE;
                  final Unfiltered next = this.sliceEndMarker();
                  if (next != null) {
                     return next;
                  }
                  continue;
               }
               case NEEDS_PRE_BLOCK: {
                  do {
                     this.filePos = this.file.getFilePointer();
                  } while (!this.preBlockStep());
                  this.stage = Stage.NEEDS_BLOCK_PREP;
               }
               case NEEDS_BLOCK_PREP: {
                  while (!this.blockPrepStep()) {
                     this.filePos = this.file.getFilePointer();
                  }
                  final Stage stage = this.stage;
                  this.stage = Stage.READY;
                  continue;
               }
            }
         }
      }
   }

   public void resetReaderState() throws IOException {
      if(this.filePos != -1L) {
         this.seekToPosition(this.filePos);
      }

   }

   public void seekToPosition(long position) throws IOException {
      this.file.seek(position);
      this.deserializer.clearState();
   }

   protected DeletionTime updateOpenMarker(RangeTombstoneMarker marker) {
      return this.openMarker = marker.isOpen(false)?marker.openDeletionTime(false):null;
   }

   public static RangeTombstoneBoundMarker markerFrom(ClusteringBound where, DeletionTime deletion) {
      if(deletion == null) {
         return null;
      } else {
         assert where != null;

         return new RangeTombstoneBoundMarker(where, deletion);
      }
   }

   public void close() throws IOException {
      if(this.shouldCloseFile && this.file != null) {
         this.file.close();
      }

   }

   protected boolean skipSmallerRow(ClusteringBound bound) throws IOException {
      assert bound != null;

      if(this.deserializer.hasNext() && this.deserializer.compareNextTo(bound) <= 0) {
         if(this.deserializer.nextIsRow()) {
            this.deserializer.skipNext();
         } else {
            this.updateOpenMarker((RangeTombstoneMarker)this.deserializer.readNext());
         }

         return false;
      } else {
         return true;
      }
   }

   protected Unfiltered readUnfiltered() throws IOException {
      assert this.end != null;

      while(this.deserializer.hasNext()) {
         if(this.deserializer.compareNextTo(this.end) >= 0) {
            this.deserializer.rewind();
            return null;
         }

         Unfiltered next = this.deserializer.readNext();
         if(!next.isEmpty()) {
            if(next.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER) {
               this.updateOpenMarker((RangeTombstoneMarker)next);
            }

            return next;
         }
      }

      return null;
   }

   public boolean setForSlice(Slice slice) throws IOException {
      this.start = slice.start();
      this.end = slice.end();
      return true;
   }

   protected boolean preSliceStep() throws IOException {
      return this.skipSmallerRow(this.start);
   }

   protected boolean slicePrepStep() throws IOException {
      return true;
   }

   protected abstract RangeTombstoneMarker sliceStartMarker();

   protected abstract Unfiltered nextInSlice() throws IOException;

   protected abstract RangeTombstoneMarker sliceEndMarker();

   protected boolean advanceBlock() throws IOException {
      return false;
   }

   protected boolean preBlockStep() throws IOException {
      throw new IllegalStateException("Should be overridden if advanceBlock is.");
   }

   protected boolean blockPrepStep() throws IOException {
      throw new IllegalStateException("Should be overridden if advanceBlock is.");
   }

   public String toString() {
      return String.format("SSTable reader class: %s, position: %d, direction: %d, slice: %d, stage: %s", new Object[]{this.getClass().getName(), Long.valueOf(this.filePos), Integer.valueOf(this.direction), Integer.valueOf(this.currentSlice), this.stage});
   }

   static enum Stage {
      NEEDS_SLICE,
      NEEDS_SET_FOR_SLICE,
      NEEDS_PRE_SLICE,
      NEEDS_SLICE_PREP,
      READY,
      NEEDS_BLOCK,
      NEEDS_PRE_BLOCK,
      NEEDS_BLOCK_PREP;

      private Stage() {
      }
   }
}
