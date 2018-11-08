package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.cassandra.io.sstable.format.ScrubPartitionIterator;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ScrubIterator extends PartitionIndex.IndexPosIterator implements ScrubPartitionIterator {
   ByteBuffer key;
   long dataPosition;
   final FileHandle rowIndexFile;

   ScrubIterator(PartitionIndex partitionIndex, FileHandle rowIndexFile) throws IOException {
      super(partitionIndex, Rebufferer.ReaderConstraint.NONE);
      this.rowIndexFile = rowIndexFile.sharedCopy();
      this.advance();
   }

   public void close() {
      super.close();
      this.rowIndexFile.close();
   }

   public ByteBuffer key() {
      return this.key;
   }

   public long dataPosition() {
      return this.dataPosition;
   }

   public void advance() throws IOException {
      long pos = this.nextIndexPos();
      if(pos != -9223372036854775808L) {
         if(pos >= 0L) {
            FileDataInput in = this.rowIndexFile.createReader(pos, Rebufferer.ReaderConstraint.NONE);
            Throwable var4 = null;

            try {
               this.key = ByteBufferUtil.readWithShortLength(in);
               this.dataPosition = TrieIndexEntry.deserialize(in, in.getFilePointer()).position;
            } catch (Throwable var13) {
               var4 = var13;
               throw var13;
            } finally {
               if(in != null) {
                  if(var4 != null) {
                     try {
                        in.close();
                     } catch (Throwable var12) {
                        var4.addSuppressed(var12);
                     }
                  } else {
                     in.close();
                  }
               }

            }
         } else {
            this.key = null;
            this.dataPosition = ~pos;
         }
      } else {
         this.key = null;
         this.dataPosition = -1L;
      }

   }
}
