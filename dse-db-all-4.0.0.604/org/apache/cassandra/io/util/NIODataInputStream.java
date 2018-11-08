package org.apache.cassandra.io.util;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.Objects;

public class NIODataInputStream extends RebufferingInputStream {
   protected final ReadableByteChannel channel;

   private static ByteBuffer makeBuffer(int bufferSize) {
      ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
      buffer.position(0);
      buffer.limit(0);
      return buffer;
   }

   public NIODataInputStream(ReadableByteChannel channel, int bufferSize) {
      super(makeBuffer(bufferSize));
      Objects.requireNonNull(channel);
      this.channel = channel;
   }

   protected void reBuffer() throws IOException {
      Preconditions.checkState(this.buffer.remaining() == 0);
      this.buffer.clear();

      while(this.channel.read(this.buffer) == 0) {
         ;
      }

      this.buffer.flip();
   }

   public void close() throws IOException {
      this.channel.close();
      super.close();
      FileUtils.clean(this.buffer);
      this.buffer = null;
   }

   public int available() throws IOException {
      if(this.channel instanceof SeekableByteChannel) {
         SeekableByteChannel sbc = (SeekableByteChannel)this.channel;
         long remainder = Math.max(0L, sbc.size() - sbc.position());
         return remainder > 2147483647L?2147483647:(int)(remainder + (long)this.buffer.remaining());
      } else {
         return this.buffer.remaining();
      }
   }
}
