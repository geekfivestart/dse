package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.cassandra.io.sstable.format.ScrubPartitionIterator;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ScrubIterator implements ScrubPartitionIterator {
   private final FileHandle ifile;
   private final RandomAccessReader reader;
   private final BigRowIndexEntry.IndexSerializer rowIndexEntrySerializer;
   private ByteBuffer key;
   private long dataPosition;

   public ScrubIterator(FileHandle ifile, BigRowIndexEntry.IndexSerializer rowIndexEntrySerializer) throws IOException {
      this.ifile = ifile.sharedCopy();
      this.reader = this.ifile.createReader();
      this.rowIndexEntrySerializer = rowIndexEntrySerializer;
      this.advance();
   }

   public void close() {
      this.reader.close();
      this.ifile.close();
   }

   public void advance() throws IOException {
      if(!this.reader.isEOF()) {
         this.key = ByteBufferUtil.readWithShortLength(this.reader);
         this.dataPosition = this.rowIndexEntrySerializer.deserializePositionAndSkip(this.reader);
      } else {
         this.dataPosition = -1L;
         this.key = null;
      }

   }

   public ByteBuffer key() {
      return this.key;
   }

   public long dataPosition() {
      return this.dataPosition;
   }
}
