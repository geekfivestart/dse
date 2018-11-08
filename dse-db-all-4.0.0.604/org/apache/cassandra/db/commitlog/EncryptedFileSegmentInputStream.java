package org.apache.cassandra.db.commitlog;

import java.io.DataInput;
import java.nio.ByteBuffer;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileSegmentInputStream;

public class EncryptedFileSegmentInputStream extends FileSegmentInputStream implements FileDataInput, DataInput {
   private final long segmentOffset;
   private final int expectedLength;
   private final EncryptedFileSegmentInputStream.ChunkProvider chunkProvider;
   private int totalChunkOffset;

   public EncryptedFileSegmentInputStream(String filePath, long segmentOffset, int position, int expectedLength, EncryptedFileSegmentInputStream.ChunkProvider chunkProvider) {
      super(chunkProvider.nextChunk(), filePath, (long)position);
      this.segmentOffset = segmentOffset;
      this.expectedLength = expectedLength;
      this.chunkProvider = chunkProvider;
   }

   public long getFilePointer() {
      return this.segmentOffset + (long)this.totalChunkOffset + (long)this.buffer.position();
   }

   public boolean isEOF() {
      return this.totalChunkOffset + this.buffer.position() >= this.expectedLength;
   }

   public long bytesRemaining() {
      return (long)(this.expectedLength - (this.totalChunkOffset + this.buffer.position()));
   }

   public void seek(long position) {
      long bufferPos;
      for(bufferPos = position - (long)this.totalChunkOffset - this.segmentOffset; this.buffer != null && bufferPos > (long)this.buffer.capacity(); bufferPos = position - (long)this.totalChunkOffset - this.segmentOffset) {
         this.buffer.position(this.buffer.limit());
         this.reBuffer();
      }

      if(this.buffer != null && bufferPos >= 0L && bufferPos <= (long)this.buffer.capacity()) {
         this.buffer.position((int)bufferPos);
      } else {
         throw new IllegalArgumentException(String.format("Unable to seek to position %d in %s (%d bytes) in partial mode", new Object[]{Long.valueOf(position), this.getPath(), Long.valueOf(this.segmentOffset + (long)this.expectedLength)}));
      }
   }

   public long bytesPastMark(DataPosition mark) {
      throw new UnsupportedOperationException();
   }

   public void reBuffer() {
      this.totalChunkOffset += this.buffer.position();
      this.buffer = this.chunkProvider.nextChunk();
   }

   public interface ChunkProvider {
      ByteBuffer nextChunk();
   }
}
