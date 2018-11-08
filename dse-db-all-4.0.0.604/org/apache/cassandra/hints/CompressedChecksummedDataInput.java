package org.apache.cassandra.hints;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.ChannelProxy;

public final class CompressedChecksummedDataInput extends ChecksummedDataInput {
   private final ICompressor compressor;
   private volatile long filePosition = 0L;
   private volatile long sourcePosition = 0L;
   private volatile ByteBuffer compressedBuffer = null;
   private final ByteBuffer metadataBuffer = ByteBuffer.allocate(8);

   public CompressedChecksummedDataInput(ChannelProxy channel, ICompressor compressor, long filePosition) {
      super(channel, compressor.preferredBufferType());
      this.compressor = compressor;
      this.sourcePosition = this.filePosition = filePosition;
   }

   public boolean isEOF() {
      return this.filePosition == this.channel.size() && this.buffer.remaining() == 0;
   }

   public long getSourcePosition() {
      return this.sourcePosition;
   }

   public InputPosition getSeekPosition() {
      return new CompressedChecksummedDataInput.Position(this.sourcePosition, this.bufferOffset, this.buffer.position());
   }

   public void seek(InputPosition p) {
      CompressedChecksummedDataInput.Position pos = (CompressedChecksummedDataInput.Position)p;
      this.bufferOffset = pos.bufferStart;
      this.filePosition = pos.sourcePosition;
      this.buffer.position(0).limit(0);
      this.resetCrc();
      this.reBuffer();
      this.buffer.position(pos.bufferPosition);

      assert this.sourcePosition == pos.sourcePosition;

      assert this.bufferOffset == pos.bufferStart;

      assert this.buffer.position() == pos.bufferPosition;

   }

   protected void readBuffer() {
      this.sourcePosition = this.filePosition;
      if(!this.isEOF()) {
         this.metadataBuffer.clear();
         this.channel.read(this.metadataBuffer, this.filePosition);
         this.filePosition += 8L;
         this.metadataBuffer.rewind();
         int uncompressedSize = this.metadataBuffer.getInt();
         int compressedSize = this.metadataBuffer.getInt();
         int bufferSize;
         if(this.compressedBuffer == null || compressedSize > this.compressedBuffer.capacity()) {
            bufferSize = compressedSize + compressedSize / 20;
            if(this.compressedBuffer != null) {
               bufferPool.put(this.compressedBuffer);
            }

            this.compressedBuffer = bufferPool.get(bufferSize, this.compressor.preferredBufferType());
         }

         this.compressedBuffer.clear();
         this.compressedBuffer.limit(compressedSize);
         this.channel.read(this.compressedBuffer, this.filePosition);
         this.compressedBuffer.rewind();
         this.filePosition += (long)compressedSize;
         if(this.buffer.capacity() < uncompressedSize) {
            bufferSize = uncompressedSize + uncompressedSize / 20;
            bufferPool.put(this.buffer);
            this.buffer = bufferPool.get(bufferSize, this.compressor.preferredBufferType());
         }

         this.buffer.clear();
         this.buffer.limit(uncompressedSize);

         try {
            this.compressor.uncompress(this.compressedBuffer, this.buffer);
            this.buffer.flip();
         } catch (IOException var4) {
            throw new FSReadError(var4, this.getPath());
         }
      }
   }

   public void close() {
      bufferPool.put(this.compressedBuffer);
      super.close();
   }

   public static ChecksummedDataInput upgradeInput(ChecksummedDataInput input, ICompressor compressor) {
      long position = input.getPosition();
      input.close();
      return new CompressedChecksummedDataInput(new ChannelProxy(input.getPath()), compressor, position);
   }

   @VisibleForTesting
   ICompressor getCompressor() {
      return this.compressor;
   }

   static class Position extends ChecksummedDataInput.Position {
      final long bufferStart;
      final int bufferPosition;

      public Position(long sourcePosition, long bufferStart, int bufferPosition) {
         super(sourcePosition);
         this.bufferStart = bufferStart;
         this.bufferPosition = bufferPosition;
      }

      public long subtract(InputPosition o) {
         CompressedChecksummedDataInput.Position other = (CompressedChecksummedDataInput.Position)o;
         return this.bufferStart - other.bufferStart + (long)this.bufferPosition - (long)other.bufferPosition;
      }
   }
}
