package org.apache.cassandra.hints;

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import javax.crypto.Cipher;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.security.EncryptionUtils;

public class EncryptedChecksummedDataInput extends ChecksummedDataInput {
   private static final FastThreadLocal<ByteBuffer> reusableBuffers = new FastThreadLocal<ByteBuffer>() {
      protected ByteBuffer initialValue() {
         return ByteBuffer.allocate(0);
      }
   };
   private final Cipher cipher;
   private final ICompressor compressor;
   private final EncryptionUtils.ChannelProxyReadChannel readChannel;
   private long sourcePosition;

   protected EncryptedChecksummedDataInput(ChannelProxy channel, Cipher cipher, ICompressor compressor, long filePosition) {
      super(channel);
      this.cipher = cipher;
      this.compressor = compressor;
      this.readChannel = new EncryptionUtils.ChannelProxyReadChannel(channel, filePosition);
      this.sourcePosition = filePosition;

      assert cipher != null;

      assert compressor != null;

   }

   public boolean isEOF() {
      return this.readChannel.getCurrentPosition() == this.channel.size() && this.buffer.remaining() == 0;
   }

   public long getSourcePosition() {
      return this.sourcePosition;
   }

   public InputPosition getSeekPosition() {
      return new EncryptedChecksummedDataInput.Position(this.sourcePosition, this.bufferOffset, this.buffer.position());
   }

   public void seek(InputPosition p) {
      EncryptedChecksummedDataInput.Position pos = (EncryptedChecksummedDataInput.Position)p;
      this.bufferOffset = pos.bufferStart;
      this.readChannel.setPosition(pos.sourcePosition);
      this.buffer.position(0).limit(0);
      this.resetCrc();
      this.reBuffer();
      this.buffer.position(pos.bufferPosition);

      assert this.sourcePosition == pos.sourcePosition;

      assert this.bufferOffset == pos.bufferStart;

      assert this.buffer.position() == pos.bufferPosition;

   }

   protected void readBuffer() {
      this.sourcePosition = this.readChannel.getCurrentPosition();
      if(!this.isEOF()) {
         try {
            ByteBuffer byteBuffer = (ByteBuffer)reusableBuffers.get();
            ByteBuffer decrypted = EncryptionUtils.decrypt((ReadableByteChannel)this.readChannel, byteBuffer, true, this.cipher);
            this.buffer = EncryptionUtils.uncompress(decrypted, this.buffer, true, this.compressor);
            if(decrypted.capacity() > byteBuffer.capacity()) {
               reusableBuffers.set(decrypted);
            }

         } catch (IOException var3) {
            throw new FSReadError(var3, this.getPath());
         }
      }
   }

   public static ChecksummedDataInput upgradeInput(ChecksummedDataInput input, Cipher cipher, ICompressor compressor) {
      long position = input.getPosition();
      input.close();
      return new EncryptedChecksummedDataInput(new ChannelProxy(input.getPath()), cipher, compressor, position);
   }

   @VisibleForTesting
   Cipher getCipher() {
      return this.cipher;
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
         EncryptedChecksummedDataInput.Position other = (EncryptedChecksummedDataInput.Position)o;
         return this.bufferStart - other.bufferStart + (long)this.bufferPosition - (long)other.bufferPosition;
      }
   }
}
