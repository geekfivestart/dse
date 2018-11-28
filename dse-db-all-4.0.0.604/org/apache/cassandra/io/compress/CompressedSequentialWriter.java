package org.apache.cassandra.io.compress;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.zip.CRC32;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.ChecksumWriter;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.Throwables;

public class CompressedSequentialWriter extends SequentialWriter {
   private final ChecksumWriter crcMetadata;
   private long chunkOffset = 0L;
   private final CompressionMetadata.Writer metadataWriter;
   private final ICompressor compressor;
   private ByteBuffer compressed;
   private int chunkCount = 0;
   private final MetadataCollector sstableMetadataCollector;
   private final ByteBuffer crcCheckBuffer = ByteBuffer.allocate(4);
   private final Optional<File> digestFile;
   private final int maxCompressedLength;

   public CompressedSequentialWriter(File file, String offsetsPath, File digestFile, SequentialWriterOption option, CompressionParams parameters, MetadataCollector sstableMetadataCollector) {
      super(file, SequentialWriterOption.newBuilder().bufferSize(parameters.chunkLength()).bufferType(parameters.getSstableCompressor().preferredBufferType()).finishOnClose(option.finishOnClose()).build());
      this.compressor = parameters.getSstableCompressor();
      this.digestFile = Optional.ofNullable(digestFile);
      this.compressed = this.compressor.preferredBufferType().allocate(this.compressor.initialCompressedBufferLength(this.buffer.capacity()));
      this.maxCompressedLength = parameters.maxCompressedLength();
      this.metadataWriter = CompressionMetadata.Writer.open(parameters, offsetsPath);
      this.sstableMetadataCollector = sstableMetadataCollector;
      this.crcMetadata = new ChecksumWriter(new DataOutputStream(Channels.newOutputStream(this.channel)));
   }

   public long getOnDiskFilePointer() {
      try {
         return this.fchannel.position();
      } catch (IOException var2) {
         throw new FSReadError(var2, this.getPath());
      }
   }

   public long getEstimatedOnDiskBytesWritten() {
      return this.chunkOffset;
   }

   public void flush() {
      throw new UnsupportedOperationException();
   }

   protected void flushData() {
      this.seekToChunkStart();

      try {
         this.buffer.flip();
         this.compressed.clear();
         this.compressor.compress(this.buffer, this.compressed);
      } catch (IOException var6) {
         throw new RuntimeException("Compression exception", var6);
      }

      int uncompressedLength = this.buffer.position();
      int compressedLength = this.compressed.position();
      ByteBuffer toWrite = this.compressed;
      if(compressedLength >= this.maxCompressedLength) {
         toWrite = this.buffer;
         compressedLength = uncompressedLength;
      }

      try {
         this.metadataWriter.addOffset(this.chunkOffset);
         ++this.chunkCount;
         toWrite.flip();
         this.channel.write(toWrite);
         toWrite.rewind();
         this.crcMetadata.appendDirect(toWrite, true);
         this.lastFlushOffset += (long)uncompressedLength;
      } catch (IOException var5) {
         throw new FSWriteError(var5, this.getPath());
      }

      if(toWrite == this.buffer) {
         this.buffer.position(compressedLength);
      }

      this.chunkOffset += (long)(compressedLength + 4);
   }

   public CompressionMetadata open(long overrideLength) {
      if(overrideLength <= 0L) {
         overrideLength = this.lastFlushOffset;
      }

      return this.metadataWriter.open(overrideLength, this.chunkOffset);
   }

   public DataPosition mark() {
      if(!this.buffer.hasRemaining()) {
         this.doFlush(0);
      }

      return new CompressedSequentialWriter.CompressedFileWriterMark(this.chunkOffset, this.current(), this.buffer.position(), this.chunkCount + 1);
   }

   public synchronized void resetAndTruncate(DataPosition mark) {
      assert mark instanceof CompressedSequentialWriter.CompressedFileWriterMark;

      CompressedSequentialWriter.CompressedFileWriterMark realMark = (CompressedSequentialWriter.CompressedFileWriterMark)mark;
      long truncateTarget = realMark.uncDataOffset;
      if(realMark.chunkOffset == this.chunkOffset) {
         this.buffer.position(realMark.validBufferBytes);
      } else {
         this.syncInternal();
         this.chunkOffset = realMark.chunkOffset;
         int chunkSize = (int)(this.metadataWriter.chunkOffsetBy(realMark.nextChunkIndex) - this.chunkOffset - 4L);
         if(this.compressed.capacity() < chunkSize) {
            this.compressed = this.compressor.preferredBufferType().allocate(chunkSize);
         }

         try {
            this.compressed.clear();
            this.compressed.limit(chunkSize);
            this.fchannel.position(this.chunkOffset);
            this.fchannel.read(this.compressed);

            try {
               this.buffer.clear();
               this.compressed.flip();
               if(chunkSize < this.maxCompressedLength) {
                  this.compressor.uncompress(this.compressed, this.buffer);
               } else {
                  this.buffer.put(this.compressed);
               }
            } catch (IOException var7) {
               throw new CorruptBlockException(this.getPath(), this.chunkOffset, chunkSize, var7);
            }

            CRC32 checksum = new CRC32();
            this.compressed.rewind();
            checksum.update(this.compressed);
            this.crcCheckBuffer.clear();
            this.fchannel.read(this.crcCheckBuffer);
            this.crcCheckBuffer.flip();
            if(this.crcCheckBuffer.getInt() != (int)checksum.getValue()) {
               throw new CorruptBlockException(this.getPath(), this.chunkOffset, chunkSize);
            }
         } catch (CorruptBlockException var8) {
            throw new CorruptSSTableException(var8, this.getPath());
         } catch (EOFException var9) {
            throw new CorruptSSTableException(new CorruptBlockException(this.getPath(), this.chunkOffset, chunkSize), this.getPath());
         } catch (IOException var10) {
            throw new FSReadError(var10, this.getPath());
         }

         this.buffer.position(realMark.validBufferBytes);
         this.bufferOffset = truncateTarget - (long)this.buffer.position();
         this.chunkCount = realMark.nextChunkIndex - 1;
         this.truncate(this.chunkOffset, this.bufferOffset);
         this.metadataWriter.resetAndTruncate(realMark.nextChunkIndex - 1);
      }
   }

   private void truncate(long toFileSize, long toBufferOffset) {
      try {
         this.fchannel.truncate(toFileSize);
         this.lastFlushOffset = toBufferOffset;
      } catch (IOException var6) {
         throw new FSWriteError(var6, this.getPath());
      }
   }

   private void seekToChunkStart() {
      if(this.getOnDiskFilePointer() != this.chunkOffset) {
         try {
            this.fchannel.position(this.chunkOffset);
         } catch (IOException var2) {
            throw new FSReadError(var2, this.getPath());
         }
      }

   }

   protected SequentialWriter.TransactionalProxy txnProxy() {
      return new CompressedSequentialWriter.TransactionalProxy();
   }

   protected static class CompressedFileWriterMark implements DataPosition {
      final long chunkOffset;
      final long uncDataOffset;
      final int validBufferBytes;
      final int nextChunkIndex;

      public CompressedFileWriterMark(long chunkOffset, long uncDataOffset, int validBufferBytes, int nextChunkIndex) {
         this.chunkOffset = chunkOffset;
         this.uncDataOffset = uncDataOffset;
         this.validBufferBytes = validBufferBytes;
         this.nextChunkIndex = nextChunkIndex;
      }
   }

   protected class TransactionalProxy extends SequentialWriter.TransactionalProxy {
      protected TransactionalProxy() {
         super();
      }

      protected Throwable doCommit(Throwable accumulate) {
         return super.doCommit(CompressedSequentialWriter.this.metadataWriter.commit(accumulate));
      }

      protected Throwable doAbort(Throwable accumulate) {
         return super.doAbort(CompressedSequentialWriter.this.metadataWriter.abort(accumulate));
      }

      protected void doPrepare() {
         CompressedSequentialWriter.this.syncInternal();
         CompressedSequentialWriter.this.digestFile.ifPresent(CompressedSequentialWriter.this.crcMetadata::writeFullChecksum);
         CompressedSequentialWriter.this.sstableMetadataCollector.addCompressionRatio(CompressedSequentialWriter.this.chunkOffset, CompressedSequentialWriter.this.lastFlushOffset);
         CompressedSequentialWriter.this.metadataWriter.finalizeLength(CompressedSequentialWriter.this.current(), CompressedSequentialWriter.this.chunkCount).prepareToCommit();
      }

      protected Throwable doPreCleanup(Throwable accumulate) {
         accumulate = super.doPreCleanup(accumulate);
         if(CompressedSequentialWriter.this.compressed != null) {
            try {
               FileUtils.clean(CompressedSequentialWriter.this.compressed);
            } catch (Throwable var3) {
               accumulate = Throwables.merge(accumulate, var3);
            }

            CompressedSequentialWriter.this.compressed = null;
         }

         return accumulate;
      }
   }
}
