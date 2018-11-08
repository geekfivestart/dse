package org.apache.cassandra.transport;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import net.jpountz.lz4.LZ4Decompressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyError;

public interface FrameCompressor {
   Frame compress(Frame var1) throws IOException;

   Frame decompress(Frame var1) throws IOException;

   public static class LZ4Compressor implements FrameCompressor {
      public static final FrameCompressor.LZ4Compressor instance = new FrameCompressor.LZ4Compressor();
      private static final int INTEGER_BYTES = 4;
      private final net.jpountz.lz4.LZ4Compressor compressor;
      private final LZ4Decompressor decompressor;

      private LZ4Compressor() {
         LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
         this.compressor = lz4Factory.fastCompressor();
         this.decompressor = lz4Factory.decompressor();
      }

      public Frame compress(Frame frame) throws IOException {
         byte[] input = CBUtil.readRawBytes(frame.body);
         int maxCompressedLength = this.compressor.maxCompressedLength(input.length);
         ByteBuf outputBuf = CBUtil.allocator.heapBuffer(4 + maxCompressedLength);
         byte[] output = outputBuf.array();
         int outputOffset = outputBuf.arrayOffset();
         output[outputOffset + 0] = (byte)(input.length >>> 24);
         output[outputOffset + 1] = (byte)(input.length >>> 16);
         output[outputOffset + 2] = (byte)(input.length >>> 8);
         output[outputOffset + 3] = (byte)input.length;

         Frame var8;
         try {
            int written = this.compressor.compress(input, 0, input.length, output, outputOffset + 4, maxCompressedLength);
            outputBuf.writerIndex(4 + written);
            var8 = frame.with(outputBuf);
         } catch (Throwable var12) {
            outputBuf.release();
            throw var12;
         } finally {
            frame.release();
         }

         return var8;
      }

      public Frame decompress(Frame frame) throws IOException {
         byte[] input = CBUtil.readRawBytes(frame.body);
         int uncompressedLength = (input[0] & 255) << 24 | (input[1] & 255) << 16 | (input[2] & 255) << 8 | input[3] & 255;
         ByteBuf output = CBUtil.allocator.heapBuffer(uncompressedLength);

         Frame var6;
         try {
            int read = this.decompressor.decompress(input, 4, output.array(), output.arrayOffset(), uncompressedLength);
            if(read != input.length - 4) {
               throw new IOException("Compressed lengths mismatch");
            }

            output.writerIndex(uncompressedLength);
            var6 = frame.with(output);
         } catch (Throwable var10) {
            output.release();
            throw var10;
         } finally {
            frame.release();
         }

         return var6;
      }
   }

   public static class SnappyCompressor implements FrameCompressor {
      public static final FrameCompressor.SnappyCompressor instance;

      private SnappyCompressor() {
         Snappy.getNativeLibraryVersion();
      }

      public Frame compress(Frame frame) throws IOException {
         byte[] input = CBUtil.readRawBytes(frame.body);
         ByteBuf output = CBUtil.allocator.heapBuffer(Snappy.maxCompressedLength(input.length));

         try {
            int written = Snappy.compress(input, 0, input.length, output.array(), output.arrayOffset());
            output.writerIndex(written);
         } catch (Throwable var8) {
            output.release();
            throw var8;
         } finally {
            frame.release();
         }

         return frame.with(output);
      }

      public Frame decompress(Frame frame) throws IOException {
         byte[] input = CBUtil.readRawBytes(frame.body);
         if(!Snappy.isValidCompressedBuffer(input, 0, input.length)) {
            throw new ProtocolException("Provided frame does not appear to be Snappy compressed");
         } else {
            ByteBuf output = CBUtil.allocator.heapBuffer(Snappy.uncompressedLength(input));

            try {
               int size = Snappy.uncompress(input, 0, input.length, output.array(), output.arrayOffset());
               output.writerIndex(size);
            } catch (Throwable var8) {
               output.release();
               throw var8;
            } finally {
               frame.release();
            }

            return frame.with(output);
         }
      }

      static {
         FrameCompressor.SnappyCompressor i;
         try {
            i = new FrameCompressor.SnappyCompressor();
         } catch (Exception var2) {
            JVMStabilityInspector.inspectThrowable(var2);
            i = null;
         } catch (SnappyError | UnsatisfiedLinkError | NoClassDefFoundError var3) {
            i = null;
         }

         instance = i;
      }
   }
}
