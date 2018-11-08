package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

public class FileSegmentInputStream extends DataInputBuffer implements FileDataInput {
   private final String filePath;
   private final long offset;

   public FileSegmentInputStream(ByteBuffer buffer, String filePath, long offset) {
      super(buffer, false);
      this.filePath = filePath;
      this.offset = offset;
   }

   public String getPath() {
      return this.filePath;
   }

   private long size() {
      return this.offset + (long)this.buffer.capacity();
   }

   public boolean isEOF() {
      return !this.buffer.hasRemaining();
   }

   public long bytesRemaining() {
      return (long)this.buffer.remaining();
   }

   public void seek(long pos) {
      if(pos >= 0L && pos <= this.size()) {
         this.buffer.position((int)(pos - this.offset));
      } else {
         throw new IllegalArgumentException(String.format("Unable to seek to position %d in %s (%d bytes) in partial mode", new Object[]{Long.valueOf(pos), this.getPath(), Long.valueOf(this.size())}));
      }
   }

   public boolean markSupported() {
      return false;
   }

   public DataPosition mark() {
      throw new UnsupportedOperationException();
   }

   public void reset(DataPosition mark) {
      throw new UnsupportedOperationException();
   }

   public long bytesPastMark(DataPosition mark) {
      return 0L;
   }

   public long getFilePointer() {
      return this.offset + (long)this.buffer.position();
   }
}
