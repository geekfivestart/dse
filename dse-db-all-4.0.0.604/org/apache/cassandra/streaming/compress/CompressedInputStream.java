package org.apache.cassandra.streaming.compress;

import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.WrappedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompressedInputStream extends InputStream {
   private static final Logger logger = LoggerFactory.getLogger(CompressedInputStream.class);
   private final CompressionInfo info;
   private final BlockingQueue<byte[]> dataBuffer;
   private final DoubleSupplier crcCheckChanceSupplier;
   private final byte[] buffer;
   protected long bufferOffset = 0L;
   private long current = 0L;
   protected int validBufferBytes = -1;
   private final ChecksumType checksumType;
   private final byte[] checksumBytes = new byte[4];
   private static final byte[] POISON_PILL = new byte[0];
   protected volatile IOException readException = null;
   private long totalCompressedBytesRead;

   public CompressedInputStream(InputStream source, CompressionInfo info, ChecksumType checksumType, DoubleSupplier crcCheckChanceSupplier) {
      this.info = info;
      this.buffer = new byte[info.parameters.chunkLength()];
      this.dataBuffer = new ArrayBlockingQueue(Math.min(info.chunks.length, 1024));
      this.crcCheckChanceSupplier = crcCheckChanceSupplier;
      this.checksumType = checksumType;
      (new FastThreadLocalThread(new CompressedInputStream.Reader(source, info, this.dataBuffer))).start();
   }

   private void decompressNextChunk() throws IOException {
      if(this.readException != null) {
         throw this.readException;
      } else {
         try {
            byte[] compressedWithCRC = (byte[])this.dataBuffer.take();
            if(compressedWithCRC == POISON_PILL) {
               assert this.readException != null;

               throw this.readException;
            } else {
               this.decompress(compressedWithCRC);
            }
         } catch (InterruptedException var2) {
            throw new EOFException("No chunk available");
         }
      }
   }

   public int read() throws IOException {
      if(this.current >= this.bufferOffset + (long)this.buffer.length || this.validBufferBytes == -1) {
         this.decompressNextChunk();
      }

      assert this.current >= this.bufferOffset && this.current < this.bufferOffset + (long)this.validBufferBytes;

      return this.buffer[(int)(this.current++ - this.bufferOffset)] & 255;
   }

   public int read(byte[] b, int off, int len) throws IOException {
      long nextCurrent = this.current + (long)len;
      if(this.current >= this.bufferOffset + (long)this.buffer.length || this.validBufferBytes == -1) {
         this.decompressNextChunk();
      }

      assert nextCurrent >= this.bufferOffset;

      int read = 0;

      while(read < len) {
         int nextLen = Math.min(len - read, (int)(this.bufferOffset + (long)this.validBufferBytes - this.current));
         System.arraycopy(this.buffer, (int)(this.current - this.bufferOffset), b, off + read, nextLen);
         read += nextLen;
         this.current += (long)nextLen;
         if(read != len) {
            this.decompressNextChunk();
         }
      }

      return len;
   }

   public void position(long position) {
      assert position >= this.current : "stream can only read forward.";

      this.current = position;
   }

   private void decompress(byte[] compressed) throws IOException {
      if(compressed.length - this.checksumBytes.length < this.info.parameters.maxCompressedLength()) {
         this.validBufferBytes = this.info.parameters.getSstableCompressor().uncompress(compressed, 0, compressed.length - this.checksumBytes.length, this.buffer, 0);
      } else {
         this.validBufferBytes = compressed.length - this.checksumBytes.length;
         System.arraycopy(compressed, 0, this.buffer, 0, this.validBufferBytes);
      }

      this.totalCompressedBytesRead += (long)compressed.length;
      double crcCheckChance = this.crcCheckChanceSupplier.getAsDouble();
      if(crcCheckChance >= 1.0D || crcCheckChance > 0.0D && crcCheckChance > ThreadLocalRandom.current().nextDouble()) {
         int checksum = (int)this.checksumType.of(compressed, 0, compressed.length - this.checksumBytes.length);
         System.arraycopy(compressed, compressed.length - this.checksumBytes.length, this.checksumBytes, 0, this.checksumBytes.length);
         if(Ints.fromByteArray(this.checksumBytes) != checksum) {
            throw new IOException("CRC unmatched");
         }
      }

      this.bufferOffset = this.current & (long)(~(this.buffer.length - 1));
   }

   public long getTotalCompressedBytesRead() {
      return this.totalCompressedBytesRead;
   }

   class Reader extends WrappedRunnable {
      private final InputStream source;
      private final Iterator<CompressionMetadata.Chunk> chunks;
      private final BlockingQueue<byte[]> dataBuffer;

      Reader(InputStream this$0, CompressionInfo source, BlockingQueue<byte[]> info) {
         this.source = source;
         this.chunks = Iterators.forArray(info.chunks);
         this.dataBuffer = dataBuffer;
      }

      protected void runMayThrow() throws Exception {
         while(this.chunks.hasNext()) {
            CompressionMetadata.Chunk chunk = (CompressionMetadata.Chunk)this.chunks.next();
            int readLength = chunk.length + 4;
            byte[] compressedWithCRC = new byte[readLength];
            int bufferRead = 0;

            while(bufferRead < readLength) {
               try {
                  int r = this.source.read(compressedWithCRC, bufferRead, readLength - bufferRead);
                  if(r < 0) {
                     CompressedInputStream.this.readException = new EOFException("No chunk available");
                     this.dataBuffer.put(CompressedInputStream.POISON_PILL);
                     return;
                  }

                  bufferRead += r;
               } catch (IOException var6) {
                  CompressedInputStream.logger.warn("Error while reading compressed input stream.", var6);
                  CompressedInputStream.this.readException = var6;
                  this.dataBuffer.put(CompressedInputStream.POISON_PILL);
                  return;
               }
            }

            this.dataBuffer.put(compressedWithCRC);
         }

      }
   }
}
