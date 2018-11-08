package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import java.util.Comparator;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.io.sstable.format.AbstractReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;

public class IndexState implements AutoCloseable {
   private final AbstractReader reader;
   private final BigRowIndexEntry indexEntry;
   private final BigRowIndexEntry.IndexInfoRetriever indexInfoRetriever;
   private final boolean reversed;
   private int currentIndexIdx;
   private int lastIndexIdx;
   private long startOfBlock;
   private Comparator<IndexInfo> indexComparator;

   public IndexState(AbstractReader reader, ClusteringComparator comparator, BigRowIndexEntry indexEntry, boolean reversed, FileHandle indexFile, Rebufferer.ReaderConstraint rc) {
      this.reader = reader;
      this.indexEntry = indexEntry;
      this.indexInfoRetriever = indexEntry.openWithIndex(indexFile, rc);
      this.reversed = reversed;
      this.currentIndexIdx = reversed?indexEntry.rowIndexCount():-1;
      this.lastIndexIdx = this.currentIndexIdx;
      this.indexComparator = reversed?(o1, o2) -> {
         return comparator.compare(o1.firstName, o2.firstName);
      }:(o1, o2) -> {
         return comparator.compare(o1.lastName, o2.lastName);
      };
   }

   public void reset() {
      this.currentIndexIdx = this.lastIndexIdx;
   }

   public boolean isDone() {
      return this.reversed?this.currentIndexIdx < 0:this.currentIndexIdx >= this.indexEntry.rowIndexCount();
   }

   public void setToBlock(int blockIdx) throws IOException {
      if(blockIdx >= 0 && blockIdx < this.indexEntry.rowIndexCount()) {
         this.reader.seekToPosition(this.columnOffset(blockIdx));
      }

      this.currentIndexIdx = blockIdx;
      this.reader.openMarker = blockIdx > 0?this.index(blockIdx - 1).endOpenMarker:null;
      this.startOfBlock = this.reader.file.getFilePointer();
      this.lastIndexIdx = this.currentIndexIdx;
   }

   private long columnOffset(int i) throws IOException {
      return this.indexEntry.position + this.index(i).offset;
   }

   public int blocksCount() {
      return this.indexEntry.rowIndexCount();
   }

   public void updateBlock() throws IOException {
      assert !this.reversed;

      if(this.currentIndexIdx < 0) {
         this.setToBlock(0);
      } else {
         while(this.currentIndexIdx + 1 < this.indexEntry.rowIndexCount() && this.isPastCurrentBlock()) {
            ++this.currentIndexIdx;
            this.startOfBlock = this.columnOffset(this.currentIndexIdx);
            this.lastIndexIdx = this.currentIndexIdx;
         }

      }
   }

   public boolean isPastCurrentBlock() throws IOException {
      assert this.reader.deserializer != null;

      return this.reader.file.getFilePointer() - this.startOfBlock >= this.currentIndex().width;
   }

   public int currentBlockIdx() {
      return this.currentIndexIdx;
   }

   public IndexInfo currentIndex() throws IOException {
      return this.index(this.currentIndexIdx);
   }

   public IndexInfo index(int i) throws IOException {
      return this.indexInfoRetriever.columnsIndex(i);
   }

   public int findBlockIndex(ClusteringBound bound, int fromIdx) throws IOException {
      return bound == ClusteringBound.BOTTOM?-1:(bound == ClusteringBound.TOP?this.blocksCount():this.indexFor(bound, fromIdx));
   }

   public int indexFor(ClusteringPrefix name, int lastIndex) throws IOException {
      IndexInfo target = new IndexInfo(name, name, 0L, 0L, (DeletionTime)null);
      int startIdx = 0;
      int endIdx = this.indexEntry.rowIndexCount() - 1;
      if(this.reversed) {
         if(lastIndex < endIdx) {
            endIdx = lastIndex;
         }
      } else if(lastIndex > 0) {
         startIdx = lastIndex;
      }

      int index = this.binarySearch(target, this.indexComparator, startIdx, endIdx);
      return index < 0?-index - (this.reversed?2:1):index;
   }

   private int binarySearch(IndexInfo key, Comparator<IndexInfo> c, int low, int high) throws IOException {
      while(true) {
         if(low <= high) {
            int mid = low + high >>> 1;
            IndexInfo midVal = this.index(mid);
            int cmp = c.compare(midVal, key);
            if(cmp < 0) {
               low = mid + 1;
               continue;
            }

            if(cmp > 0) {
               high = mid - 1;
               continue;
            }

            return mid;
         }

         return -(low + 1);
      }
   }

   public String toString() {
      return String.format("IndexState(indexSize=%d, currentBlock=%d, reversed=%b)", new Object[]{Integer.valueOf(this.indexEntry.rowIndexCount()), Integer.valueOf(this.currentIndexIdx), Boolean.valueOf(this.reversed)});
   }

   public void close() throws IOException {
      this.indexInfoRetriever.close();
   }
}
