package org.apache.cassandra.io.compress;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public abstract class ChainedCompressor implements ICompressor {
   private static final int HEADER_SIZE = 4;
   private static final int BUFFER_TTL = 1000;
   private static final BufferRecycler bufferRecycler;
   private final List<ICompressor> compressors;

   public ChainedCompressor(List<ICompressor> compressors) {
      this.compressors = compressors;
   }

   public int initialCompressedBufferLength(int chunkLength) {
      int inputSize = chunkLength;
      int maxOutputSize = 0;

      int outputSize;
      for(Iterator var4 = this.compressors.iterator(); var4.hasNext(); inputSize = outputSize) {
         ICompressor c = (ICompressor)var4.next();
         outputSize = c.initialCompressedBufferLength(inputSize) + 4;
         maxOutputSize = Math.max(maxOutputSize, outputSize);
      }

      return maxOutputSize;
   }

   public void compress(ByteBuffer input, ByteBuffer output) throws IOException {
      ChainedCompressor.CompressionBuffer buffer = new ChainedCompressor.ExternalCompressionBuffer(input);

      ICompressor compressor;
      for(Iterator var4 = this.compressors.iterator(); var4.hasNext(); buffer = this.compress((ICompressor)compressor, (ChainedCompressor.CompressionBuffer)buffer)) {
         compressor = (ICompressor)var4.next();
      }

      try {
         output.put(((ChainedCompressor.CompressionBuffer)buffer).buffer());
      } catch (Exception var9) {
         throw new IOException(var9);
      } finally {
         ((ChainedCompressor.CompressionBuffer)buffer).recycle();
      }

   }

   public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException {
      ChainedCompressor.CompressionBuffer buffer = new ChainedCompressor.InternalCompressionBuffer(inputLength, this.preferredBufferType());
      ((ChainedCompressor.CompressionBuffer)buffer).put(input, inputOffset, inputLength);
      ((ChainedCompressor.CompressionBuffer)buffer).flip();

      ICompressor compressor;
      for(Iterator var7 = Lists.reverse(this.compressors).iterator(); var7.hasNext(); buffer = this.uncompress((ICompressor)compressor, (ChainedCompressor.CompressionBuffer)buffer)) {
         compressor = (ICompressor)var7.next();
      }

      int outputLength = ((ChainedCompressor.CompressionBuffer)buffer).remaining();
      ((ChainedCompressor.CompressionBuffer)buffer).get(output, outputOffset, ((ChainedCompressor.CompressionBuffer)buffer).remaining());
      ((ChainedCompressor.CompressionBuffer)buffer).recycle();
      return outputLength;
   }

   public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException {
      ChainedCompressor.CompressionBuffer buffer = new ChainedCompressor.ExternalCompressionBuffer(input);

      ICompressor compressor;
      for(Iterator var4 = Lists.reverse(this.compressors).iterator(); var4.hasNext(); buffer = this.uncompress((ICompressor)compressor, (ChainedCompressor.CompressionBuffer)buffer)) {
         compressor = (ICompressor)var4.next();
      }

      try {
         output.put(((ChainedCompressor.CompressionBuffer)buffer).buffer());
      } catch (Exception var9) {
         throw new IOException(var9);
      } finally {
         ((ChainedCompressor.CompressionBuffer)buffer).recycle();
      }

   }

   public BufferType preferredBufferType() {
      return this.supports(BufferType.OFF_HEAP)?BufferType.OFF_HEAP:BufferType.ON_HEAP;
   }

   public boolean supports(BufferType bufferType) {
      Iterator var2 = this.compressors.iterator();

      ICompressor compressor;
      do {
         if(!var2.hasNext()) {
            return true;
         }

         compressor = (ICompressor)var2.next();
      } while(compressor.supports(bufferType));

      return false;
   }

   private ChainedCompressor.CompressionBuffer compress(ICompressor compressor, ChainedCompressor.CompressionBuffer input) throws IOException {
      int inputLength = input.remaining();
      int predictedLength = compressor.initialCompressedBufferLength(inputLength) + 4;
      ChainedCompressor.CompressionBuffer output = new ChainedCompressor.InternalCompressionBuffer(predictedLength, this.preferredBufferType());
      output.putInt(inputLength);
      compressor.compress(input.buffer(), output.buffer());
      output.flip();
      input.recycle();
      return output;
   }

   private ChainedCompressor.CompressionBuffer uncompress(ICompressor compressor, ChainedCompressor.CompressionBuffer input) throws IOException {
      int outputLength = input.getInt();
      ChainedCompressor.CompressionBuffer output = new ChainedCompressor.InternalCompressionBuffer(outputLength, this.preferredBufferType());
      compressor.uncompress(input.buffer(), output.buffer());
      output.flip();

      assert outputLength == output.remaining() : "Invalid uncompressed length: " + outputLength + " != " + output.remaining();

      input.recycle();
      return output;
   }

   public Set<String> supportedOptions() {
      Set<String> options = Sets.newHashSet();
      Iterator var2 = this.compressors.iterator();

      while(var2.hasNext()) {
         ICompressor c = (ICompressor)var2.next();
         options.addAll(c.supportedOptions());
      }

      return options;
   }

   static {
      bufferRecycler = BufferRecycler.instance;
   }

   private static class ExternalCompressionBuffer extends ChainedCompressor.CompressionByteBuffer {
      public ExternalCompressionBuffer(ByteBuffer buffer) {
         super(buffer);
      }

      public void recycle() {
      }
   }

   private static class InternalCompressionBuffer extends ChainedCompressor.CompressionByteBuffer {
      public InternalCompressionBuffer(int minCapacity, BufferType type) {
         super(ChainedCompressor.bufferRecycler.allocate(minCapacity, type));
      }

      public void recycle() {
         ChainedCompressor.bufferRecycler.recycle(this.buffer, 1000);
      }
   }

   private abstract static class CompressionByteBuffer implements ChainedCompressor.CompressionBuffer {
      protected ByteBuffer buffer;

      public CompressionByteBuffer(ByteBuffer buffer) {
         this.buffer = buffer;
      }

      public ByteBuffer buffer() {
         return this.buffer;
      }

      public int limit() {
         return this.buffer.limit();
      }

      public int remaining() {
         return this.buffer.remaining();
      }

      public void get(byte[] dst, int offset, int length) {
         this.buffer.get(dst, offset, length);
      }

      public void put(byte[] src, int offset, int length) {
         this.buffer.put(src, offset, length);
      }

      public int getInt() {
         return this.buffer.getInt();
      }

      public void putInt(int value) {
         this.buffer.putInt(value);
      }

      public Buffer flip() {
         return this.buffer.flip();
      }
   }

   private interface CompressionBuffer {
      ByteBuffer buffer();

      int limit();

      int remaining();

      void recycle();

      Buffer flip();

      void putInt(int var1);

      void put(byte[] var1, int var2, int var3);

      int getInt();

      void get(byte[] var1, int var2, int var3);
   }
}
