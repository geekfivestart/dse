package org.apache.cassandra.hints;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.utils.memory.BufferPool;

public class ChecksummedDataInput extends RebufferingInputStream {
   protected static final BufferPool bufferPool = new BufferPool();
   private final CRC32 crc;
   private int crcPosition;
   private boolean crcUpdateDisabled;
   private long limit;
   private long limitMark;
   protected long bufferOffset;
   protected final ChannelProxy channel;

   ChecksummedDataInput(ChannelProxy channel, BufferType bufferType) {
      super(bufferPool.get(4096, bufferType));
      this.crc = new CRC32();
      this.crcPosition = 0;
      this.crcUpdateDisabled = false;
      this.channel = channel;
      this.bufferOffset = 0L;
      this.buffer.limit(0);
      this.resetLimit();
   }

   ChecksummedDataInput(ChannelProxy channel) {
      this(channel, BufferType.OFF_HEAP);
   }

   public static ChecksummedDataInput open(File file) {
      return new ChecksummedDataInput(new ChannelProxy(file));
   }

   public boolean isEOF() {
      return this.getPosition() == this.channel.size();
   }

   public InputPosition getSeekPosition() {
      return new ChecksummedDataInput.Position(this.getPosition());
   }

   public void seek(InputPosition pos) {
      this.updateCrc();
      this.bufferOffset = ((ChecksummedDataInput.Position)pos).sourcePosition;
      this.buffer.position(0).limit(0);
   }

   public void resetCrc() {
      this.crc.reset();
      this.crcPosition = this.buffer.position();
   }

   public void limit(long newLimit) {
      this.limitMark = this.getPosition();
      this.limit = this.limitMark + newLimit;
   }

   protected long getPosition() {
      return this.bufferOffset + (long)this.buffer.position();
   }

   protected long getSourcePosition() {
      return this.bufferOffset;
   }

   public void resetLimit() {
      this.limit = 9223372036854775807L;
      this.limitMark = -1L;
   }

   public void checkLimit(int length) throws IOException {
      if(this.getPosition() + (long)length > this.limit) {
         throw new IOException("Digest mismatch exception");
      }
   }

   public long bytesPastLimit() {
      assert this.limitMark != -1L;

      return this.getPosition() - this.limitMark;
   }

   public boolean checkCrc() throws IOException {
      boolean var1;
      try {
         this.updateCrc();
         this.crcUpdateDisabled = true;
         var1 = (int)this.crc.getValue() == this.readInt();
      } finally {
         this.crcPosition = this.buffer.position();
         this.crcUpdateDisabled = false;
      }

      return var1;
   }

   public void readFully(byte[] b) throws IOException {
      this.checkLimit(b.length);
      super.readFully(b);
   }

   public int read(byte[] b, int off, int len) throws IOException {
      this.checkLimit(len);
      return super.read(b, off, len);
   }

   protected void reBuffer() {
      Preconditions.checkState(this.buffer.remaining() == 0);
      this.updateCrc();
      this.bufferOffset += (long)this.buffer.limit();
      this.readBuffer();
      this.crcPosition = this.buffer.position();
   }

   protected void readBuffer() {
      this.buffer.clear();

      while(this.channel.read(this.buffer, this.bufferOffset) == 0) {
         ;
      }

      this.buffer.flip();
   }

   public void tryUncacheRead() {
      this.getChannel().tryToSkipCache(0L, this.getSourcePosition());
   }

   private void updateCrc() {
      if(this.crcPosition != this.buffer.position() && !this.crcUpdateDisabled) {
         assert this.crcPosition >= 0 && this.crcPosition < this.buffer.position();

         ByteBuffer unprocessed = this.buffer.duplicate();
         unprocessed.position(this.crcPosition).limit(this.buffer.position());
         this.crc.update(unprocessed);
      }
   }

   public void close() {
      bufferPool.put(this.buffer);
      this.channel.close();
   }

   protected String getPath() {
      return this.channel.filePath();
   }

   public ChannelProxy getChannel() {
      return this.channel;
   }

   static class Position implements InputPosition {
      final long sourcePosition;

      public Position(long sourcePosition) {
         this.sourcePosition = sourcePosition;
      }

      public long subtract(InputPosition other) {
         return this.sourcePosition - ((ChecksummedDataInput.Position)other).sourcePosition;
      }
   }
}
