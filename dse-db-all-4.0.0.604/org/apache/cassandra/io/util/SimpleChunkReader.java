package org.apache.cassandra.io.util;

import com.google.common.primitives.Ints;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SimpleChunkReader extends AbstractReaderFileProxy implements ChunkReader {
   private static final Logger logger = LoggerFactory.getLogger(SimpleChunkReader.class);
   private final int bufferSize;
   private final BufferType bufferType;

   SimpleChunkReader(AsynchronousChannelProxy channel, long fileLength, BufferType bufferType, int bufferSize) {
      super(channel, fileLength);
      this.bufferSize = bufferSize;
      this.bufferType = TPC.USE_AIO?BufferType.OFF_HEAP_ALIGNED:bufferType;
   }

   private boolean isAlignedRead(long position, ByteBuffer buffer) {
      int blockMask = this.sectorSize - 1;
      return (position & (long)blockMask) == 0L && buffer.isDirect() && (buffer.capacity() & blockMask) == 0 && (UnsafeByteBufferAccess.getAddress(buffer) & (long)blockMask) == 0L;
   }

   public CompletableFuture<ByteBuffer> readChunk(long position, ByteBuffer buffer) {
      return this.channel.requiresAlignment() && !this.isAlignedRead(position, buffer)?this.readChunkWithAlignment(position, buffer):this.readChunkDirect(position, buffer);
   }

   private CompletableFuture<ByteBuffer> readChunkDirect(long position, final ByteBuffer buffer) {
      buffer.clear();
      final CompletableFuture<ByteBuffer> futureBuffer = new CompletableFuture();
      this.channel.read(buffer, position, new CompletionHandler<Integer, ByteBuffer>() {
         public void completed(Integer result, ByteBuffer attachment) {
            buffer.flip();
            if(!futureBuffer.complete(buffer)) {
               SimpleChunkReader.logger.warn("Failed to complete read from {}, already timed out.", SimpleChunkReader.this.channel.filePath);
            }

         }

         public void failed(Throwable exc, ByteBuffer attachment) {
            buffer.position(0).limit(0);
            futureBuffer.completeExceptionally(new CorruptSSTableException(exc, SimpleChunkReader.this.channel.filePath()));
         }
      });
      return futureBuffer;
   }

   private CompletableFuture<ByteBuffer> readChunkWithAlignment(long position, final ByteBuffer buffer) {
      buffer.clear();
      final ChunkReader.BufferHandle scratchHandle = this.getScratchHandle();
      final ByteBuffer scratchBuffer = scratchHandle.get(buffer.capacity() + this.sectorSize * 2 - 1 & -this.sectorSize);
      final CompletableFuture<ByteBuffer> futureBuffer = new CompletableFuture();
      long alignedOffset = this.roundDownToBlockSize(position);
      final int alignmentShift = Ints.checkedCast(position - alignedOffset);
      scratchBuffer.clear();
      scratchBuffer.limit(this.roundUpToBlockSize(buffer.capacity() + alignmentShift));
      this.channel.read(scratchBuffer, alignedOffset, new CompletionHandler<Integer, ByteBuffer>() {
         public void completed(Integer result, ByteBuffer attachment) {
            buffer.clear();
            if(result.intValue() >= alignmentShift) {
               scratchBuffer.limit(Math.min(result.intValue(), buffer.capacity() + alignmentShift));
               scratchBuffer.position(alignmentShift);
               buffer.put(scratchBuffer);
            }

            buffer.flip();
            scratchHandle.recycle();
            if(!futureBuffer.complete(buffer)) {
               SimpleChunkReader.logger.warn("Failed to complete read from {}, already timed out.", SimpleChunkReader.this.channel.filePath);
            }

         }

         public void failed(Throwable exc, ByteBuffer attachment) {
            buffer.position(0).limit(0);
            scratchHandle.recycle();
            futureBuffer.completeExceptionally(new CorruptSSTableException(exc, SimpleChunkReader.this.channel.filePath()));
         }
      });
      return futureBuffer;
   }

   public int chunkSize() {
      return this.bufferSize;
   }

   public BufferType preferredBufferType() {
      return this.bufferType;
   }

   public boolean isMmap() {
      return false;
   }

   public ChunkReader withChannel(AsynchronousChannelProxy channel) {
      return new SimpleChunkReader(channel, this.fileLength, this.bufferType, this.bufferSize);
   }

   public Rebufferer instantiateRebufferer(FileAccessType accessType) {
      if(accessType != FileAccessType.RANDOM && Integer.bitCount(this.bufferSize) == 1 && PrefetchingRebufferer.READ_AHEAD_SIZE_KB > 0) {
         AsynchronousChannelProxy channel = this.channel.maybeBatched(PrefetchingRebufferer.READ_AHEAD_VECTORED);
         return new PrefetchingRebufferer(this.instantiateRebufferer(this.withChannel(channel)), channel);
      } else {
         return this.instantiateRebufferer((ChunkReader)this);
      }
   }

   private Rebufferer instantiateRebufferer(ChunkReader reader) {
      return (Rebufferer)(Integer.bitCount(this.bufferSize) == 1?new BufferManagingRebufferer.Aligned(reader):new BufferManagingRebufferer.Unaligned(reader));
   }

   public String toString() {
      return String.format("%s(%s - chunk length %d, data length %d)", new Object[]{this.getClass().getSimpleName(), this.channel.filePath(), Integer.valueOf(this.bufferSize), Long.valueOf(this.fileLength())});
   }
}
