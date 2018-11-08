package org.apache.cassandra.io.compress;

import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public final class DeflateCompressor implements ICompressor {
   public static final DeflateCompressor instance = new DeflateCompressor();
   private static final FastThreadLocal<byte[]> threadLocalScratchBuffer = new FastThreadLocal<byte[]>() {
      protected byte[] initialValue() {
         return new byte[65536];
      }
   };
   private static final FastThreadLocal<Deflater> deflater = new FastThreadLocal<Deflater>() {
      protected Deflater initialValue() {
         return new Deflater();
      }
   };
   private static final FastThreadLocal<Inflater> inflater = new FastThreadLocal<Inflater>() {
      protected Inflater initialValue() {
         return new Inflater();
      }
   };

   public DeflateCompressor() {
   }

   public static byte[] getThreadLocalScratchBuffer() {
      return (byte[])threadLocalScratchBuffer.get();
   }

   public static DeflateCompressor create(Map<String, String> compressionOptions) {
      return instance;
   }

   public Set<String> supportedOptions() {
      return Collections.emptySet();
   }

   public int initialCompressedBufferLength(int sourceLen) {
      return sourceLen + (sourceLen >> 12) + (sourceLen >> 14) + (sourceLen >> 25) + 13;
   }

   public void compress(ByteBuffer input, ByteBuffer output) {
      if(input.hasArray() && output.hasArray()) {
         int length = this.compressArray(input.array(), input.arrayOffset() + input.position(), input.remaining(), output.array(), output.arrayOffset() + output.position(), output.remaining());
         input.position(input.limit());
         output.position(output.position() + length);
      } else {
         this.compressBuffer(input, output);
      }

   }

   public int compressArray(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength) {
      Deflater def = (Deflater)deflater.get();
      def.reset();
      def.setInput(input, inputOffset, inputLength);
      def.finish();
      if(def.needsInput()) {
         return 0;
      } else {
         int len = def.deflate(output, outputOffset, maxOutputLength);

         assert def.finished();

         return len;
      }
   }

   public void compressBuffer(ByteBuffer input, ByteBuffer output) {
      Deflater def = (Deflater)deflater.get();
      def.reset();
      byte[] buffer = getThreadLocalScratchBuffer();
      int chunkLen = buffer.length / 2;

      int len;
      while(input.remaining() > chunkLen) {
         input.get(buffer, 0, chunkLen);
         def.setInput(buffer, 0, chunkLen);

         while(!def.needsInput()) {
            len = def.deflate(buffer, chunkLen, chunkLen);
            output.put(buffer, chunkLen, len);
         }
      }

      len = input.remaining();
      input.get(buffer, 0, len);
      def.setInput(buffer, 0, len);
      def.finish();

      while(!def.finished()) {
         int len = def.deflate(buffer, chunkLen, chunkLen);
         output.put(buffer, chunkLen, len);
      }

   }

   public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException {
      if(input.hasArray() && output.hasArray()) {
         int length = this.uncompress(input.array(), input.arrayOffset() + input.position(), input.remaining(), output.array(), output.arrayOffset() + output.position(), output.remaining());
         input.position(input.limit());
         output.position(output.position() + length);
      } else {
         this.uncompressBuffer(input, output);
      }

   }

   public void uncompressBuffer(ByteBuffer input, ByteBuffer output) throws IOException {
      try {
         Inflater inf = (Inflater)inflater.get();
         inf.reset();
         byte[] buffer = getThreadLocalScratchBuffer();
         int chunkLen = buffer.length / 2;

         int len;
         while(input.remaining() > chunkLen) {
            input.get(buffer, 0, chunkLen);
            inf.setInput(buffer, 0, chunkLen);

            while(!inf.needsInput()) {
               len = inf.inflate(buffer, chunkLen, chunkLen);
               output.put(buffer, chunkLen, len);
            }
         }

         len = input.remaining();
         input.get(buffer, 0, len);
         inf.setInput(buffer, 0, len);

         while(!inf.needsInput()) {
            int len = inf.inflate(buffer, chunkLen, chunkLen);
            output.put(buffer, chunkLen, len);
         }

      } catch (DataFormatException var8) {
         throw new IOException(var8);
      }
   }

   public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException {
      return this.uncompress(input, inputOffset, inputLength, output, outputOffset, output.length - outputOffset);
   }

   public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength) throws IOException {
      Inflater inf = (Inflater)inflater.get();
      inf.reset();
      inf.setInput(input, inputOffset, inputLength);
      if(inf.needsInput()) {
         return 0;
      } else {
         try {
            return inf.inflate(output, outputOffset, maxOutputLength);
         } catch (DataFormatException var9) {
            throw new IOException(var9);
         }
      }
   }

   public boolean supports(BufferType bufferType) {
      return true;
   }

   public BufferType preferredBufferType() {
      return BufferType.ON_HEAP;
   }
}
