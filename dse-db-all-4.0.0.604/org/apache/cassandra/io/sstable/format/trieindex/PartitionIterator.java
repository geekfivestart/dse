package org.apache.cassandra.io.sstable.format.trieindex;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

class PartitionIterator extends PartitionIndex.IndexPosIterator implements PartitionIndexIterator {
   final PartitionPosition limit;
   final int exclusiveLimit;
   DecoratedKey currentKey;
   RowIndexEntry currentEntry;
   DecoratedKey nextKey;
   RowIndexEntry nextEntry;
   final FileHandle dataFile;
   final FileHandle rowIndexFile;
   final PartitionIndex partitionIndex;
   final IPartitioner partitioner;
   boolean closeHandles = false;

   PartitionIterator(PartitionIndex partitionIndex, IPartitioner partitioner, FileHandle rowIndexFile, FileHandle dataFile, PartitionPosition left, int inclusiveLeft, PartitionPosition right, int exclusiveRight, Rebufferer.ReaderConstraint rc) throws IOException {
      super(partitionIndex, left, right, rc);
      this.partitionIndex = partitionIndex;
      this.partitioner = partitioner;
      this.limit = right;
      this.exclusiveLimit = exclusiveRight;
      this.rowIndexFile = rowIndexFile;
      this.dataFile = dataFile;
      this.readNext();
      if(this.nextKey != null && this.nextKey.compareTo(left) <= inclusiveLeft) {
         this.readNext();
      }

      this.advance();
   }

   PartitionIterator(PartitionIndex partitionIndex, IPartitioner partitioner, FileHandle rowIndexFile, FileHandle dataFile, Rebufferer.ReaderConstraint rc) throws IOException {
      super(partitionIndex, rc);
      this.partitionIndex = partitionIndex;
      this.partitioner = partitioner;
      this.limit = null;
      this.exclusiveLimit = 0;
      this.rowIndexFile = rowIndexFile;
      this.dataFile = dataFile;
      this.readNext();
      this.advance();
   }

   public PartitionIterator closeHandles() {
      this.closeHandles = true;
      return this;
   }

   public void close() {
      try {
         if(this.closeHandles) {
            FBUtilities.closeAll(ImmutableList.of(this.partitionIndex, this.dataFile, this.rowIndexFile));
         }
      } catch (Exception var5) {
         Throwables.propagate(var5);
      } finally {
         super.close();
      }

   }

   public DecoratedKey key() {
      return this.currentKey;
   }

   public long dataPosition() {
      return this.currentEntry != null?this.currentEntry.position:-1L;
   }

   public RowIndexEntry entry() {
      return this.currentEntry;
   }

   public void advance() throws IOException {
      this.currentKey = this.nextKey;
      this.currentEntry = this.nextEntry;
      if(this.currentKey != null) {
         this.readNext();
         if(this.nextKey == null && this.limit != null && this.currentKey.compareTo(this.limit) > this.exclusiveLimit) {
            this.currentKey = null;
            this.currentEntry = null;
         }
      }

   }

   private void readNext() throws IOException {
      long pos = this.nextIndexPos();
      if(pos != -9223372036854775808L) {
         FileDataInput in;
         Throwable var4;
         if(pos >= 0L) {
            in = this.rowIndexFile.createReader(pos, this.rc);
            var4 = null;

            try {
               this.nextKey = this.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in));
               this.nextEntry = TrieIndexEntry.deserialize(in, in.getFilePointer());
            } catch (Throwable var28) {
               var4 = var28;
               throw var28;
            } finally {
               if(in != null) {
                  if(var4 != null) {
                     try {
                        in.close();
                     } catch (Throwable var25) {
                        var4.addSuppressed(var25);
                     }
                  } else {
                     in.close();
                  }
               }

            }
         } else {
            in = this.dataFile.createReader(~pos, this.rc);
            var4 = null;

            try {
               this.nextKey = this.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in));
               this.nextEntry = new RowIndexEntry(~pos);
            } catch (Throwable var27) {
               var4 = var27;
               throw var27;
            } finally {
               if(in != null) {
                  if(var4 != null) {
                     try {
                        in.close();
                     } catch (Throwable var26) {
                        var4.addSuppressed(var26);
                     }
                  } else {
                     in.close();
                  }
               }

            }
         }
      } else {
         this.nextKey = null;
         this.nextEntry = null;
      }

   }
}
