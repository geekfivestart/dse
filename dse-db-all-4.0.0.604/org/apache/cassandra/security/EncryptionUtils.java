package org.apache.cassandra.security;

import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Objects;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.ByteBufferUtil;

public class EncryptionUtils {
   public static final int COMPRESSED_BLOCK_HEADER_SIZE = 4;
   public static final int ENCRYPTED_BLOCK_HEADER_SIZE = 8;
   private static final FastThreadLocal<ByteBuffer> reusableBuffers = new FastThreadLocal<ByteBuffer>() {
      protected ByteBuffer initialValue() {
         return ByteBuffer.allocate(8);
      }
   };

   public EncryptionUtils() {
   }

   public static ByteBuffer compress(ByteBuffer inputBuffer, ByteBuffer outputBuffer, boolean allowBufferResize, ICompressor compressor) throws IOException {
      int inputLength = inputBuffer.remaining();
      int compressedLength = compressor.initialCompressedBufferLength(inputLength);
      outputBuffer = ByteBufferUtil.ensureCapacity(outputBuffer, compressedLength + 4, allowBufferResize);
      outputBuffer.putInt(inputLength);
      compressor.compress(inputBuffer, outputBuffer);
      outputBuffer.flip();
      return outputBuffer;
   }

   public static ByteBuffer encryptAndWrite(ByteBuffer inputBuffer, WritableByteChannel channel, boolean allowBufferResize, Cipher cipher) throws IOException {
      int plainTextLength = inputBuffer.remaining();
      int encryptLength = cipher.getOutputSize(plainTextLength);
      ByteBuffer outputBuffer = inputBuffer.duplicate();
      outputBuffer = ByteBufferUtil.ensureCapacity(outputBuffer, encryptLength, allowBufferResize);
      ByteBuffer intBuf = ByteBuffer.allocate(8);
      intBuf.putInt(0, encryptLength);
      intBuf.putInt(4, plainTextLength);
      channel.write(intBuf);

      try {
         cipher.doFinal(inputBuffer, outputBuffer);
      } catch (IllegalBlockSizeException | BadPaddingException | ShortBufferException var9) {
         throw new IOException("failed to encrypt commit log block", var9);
      }

      outputBuffer.position(0).limit(encryptLength);
      channel.write(outputBuffer);
      outputBuffer.position(0).limit(encryptLength);
      return outputBuffer;
   }

   public static ByteBuffer encrypt(ByteBuffer inputBuffer, ByteBuffer outputBuffer, boolean allowBufferResize, Cipher cipher) throws IOException {
      Objects.requireNonNull(outputBuffer, "output buffer may not be null");
      return encryptAndWrite(inputBuffer, new EncryptionUtils.ChannelAdapter(outputBuffer, null), allowBufferResize, cipher);
   }

   public static ByteBuffer decrypt(ReadableByteChannel channel, ByteBuffer outputBuffer, boolean allowBufferResize, Cipher cipher) throws IOException {
      ByteBuffer metadataBuffer = (ByteBuffer)reusableBuffers.get();
      if(metadataBuffer.capacity() < 8) {
         metadataBuffer = ByteBufferUtil.ensureCapacity(metadataBuffer, 8, true);
         reusableBuffers.set(metadataBuffer);
      }

      metadataBuffer.position(0).limit(8);
      channel.read(metadataBuffer);
      if(metadataBuffer.remaining() < 8) {
         throw new IllegalStateException("could not read encrypted blocked metadata header");
      } else {
         int encryptedLength = metadataBuffer.getInt();
         int plainTextLength = metadataBuffer.getInt();
         outputBuffer = ByteBufferUtil.ensureCapacity(outputBuffer, Math.max(plainTextLength, encryptedLength), allowBufferResize);
         outputBuffer.position(0).limit(encryptedLength);
         channel.read(outputBuffer);
         ByteBuffer dupe = outputBuffer.duplicate();
         dupe.clear();

         try {
            cipher.doFinal(outputBuffer, dupe);
         } catch (IllegalBlockSizeException | BadPaddingException | ShortBufferException var9) {
            throw new IOException("failed to decrypt commit log block", var9);
         }

         dupe.position(0).limit(plainTextLength);
         return dupe;
      }
   }

   public static ByteBuffer decrypt(FileDataInput fileDataInput, ByteBuffer outputBuffer, boolean allowBufferResize, Cipher cipher) throws IOException {
      return decrypt((ReadableByteChannel)(new EncryptionUtils.DataInputReadChannel(fileDataInput, null)), outputBuffer, allowBufferResize, cipher);
   }

   public static ByteBuffer uncompress(ByteBuffer inputBuffer, ByteBuffer outputBuffer, boolean allowBufferResize, ICompressor compressor) throws IOException {
      int outputLength = inputBuffer.getInt();
      outputBuffer = ByteBufferUtil.ensureCapacity(outputBuffer, outputLength, allowBufferResize);
      compressor.uncompress(inputBuffer, outputBuffer);
      outputBuffer.position(0).limit(outputLength);
      return outputBuffer;
   }

   public static int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, ICompressor compressor) throws IOException {
      int outputLength = readInt(input, inputOffset);
      inputOffset += 4;
      inputLength -= 4;
      if(output.length - outputOffset < outputLength) {
         String msg = String.format("buffer to uncompress into is not large enough; buf size = %d, buf offset = %d, target size = %s", new Object[]{Integer.valueOf(output.length), Integer.valueOf(outputOffset), Integer.valueOf(outputLength)});
         throw new IllegalStateException(msg);
      } else {
         return compressor.uncompress(input, inputOffset, inputLength, output, outputOffset);
      }
   }

   private static int readInt(byte[] input, int inputOffset) {
      return input[inputOffset + 3] & 255 | (input[inputOffset + 2] & 255) << 8 | (input[inputOffset + 1] & 255) << 16 | (input[inputOffset] & 255) << 24;
   }

   public static class ChannelProxyReadChannel implements ReadableByteChannel {
      private final ChannelProxy channelProxy;
      private volatile long currentPosition;

      public ChannelProxyReadChannel(ChannelProxy channelProxy, long currentPosition) {
         this.channelProxy = channelProxy;
         this.currentPosition = currentPosition;
      }

      public int read(ByteBuffer dst) throws IOException {
         int bytesRead = this.channelProxy.read(dst, this.currentPosition);
         dst.flip();
         this.currentPosition += (long)bytesRead;
         return bytesRead;
      }

      public long getCurrentPosition() {
         return this.currentPosition;
      }

      public boolean isOpen() {
         return this.channelProxy.isCleanedUp();
      }

      public void close() {
      }

      public void setPosition(long sourcePosition) {
         this.currentPosition = sourcePosition;
      }
   }

   private static class DataInputReadChannel implements ReadableByteChannel {
      private final FileDataInput fileDataInput;

      private DataInputReadChannel(FileDataInput dataInput) {
         this.fileDataInput = dataInput;
      }

      public int read(ByteBuffer dst) throws IOException {
         int readLength = dst.remaining();
         this.fileDataInput.readFully(dst.array(), dst.position(), readLength);
         return readLength;
      }

      public boolean isOpen() {
         try {
            return this.fileDataInput.isEOF();
         } catch (IOException var2) {
            return true;
         }
      }

      public void close() {
      }
   }

   private static final class ChannelAdapter implements WritableByteChannel {
      private final ByteBuffer buffer;

      private ChannelAdapter(ByteBuffer buffer) {
         this.buffer = buffer;
      }

      public int write(ByteBuffer src) {
         int count = src.remaining();
         this.buffer.put(src);
         return count;
      }

      public boolean isOpen() {
         return true;
      }

      public void close() {
      }
   }
}
