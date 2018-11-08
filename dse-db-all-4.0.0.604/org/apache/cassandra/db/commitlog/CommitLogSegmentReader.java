package org.apache.cassandra.db.commitlog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.zip.CRC32;
import javax.crypto.Cipher;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileSegmentInputStream;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class CommitLogSegmentReader implements Iterable<CommitLogSegmentReader.SyncSegment> {
   private final CommitLogReadHandler handler;
   private final CommitLogDescriptor descriptor;
   private final RandomAccessReader reader;
   private final CommitLogSegmentReader.Segmenter segmenter;
   private final boolean tolerateTruncation;
   protected int end;

   protected CommitLogSegmentReader(CommitLogReadHandler handler, CommitLogDescriptor descriptor, RandomAccessReader reader, boolean tolerateTruncation) {
      this.handler = handler;
      this.descriptor = descriptor;
      this.reader = reader;
      this.tolerateTruncation = tolerateTruncation;
      this.end = (int)reader.getFilePointer();
      if(descriptor.getEncryptionContext().isEnabled()) {
         this.segmenter = new CommitLogSegmentReader.EncryptedSegmenter(descriptor, reader);
      } else if(descriptor.compression != null) {
         this.segmenter = new CommitLogSegmentReader.CompressedSegmenter(descriptor, reader);
      } else {
         this.segmenter = new CommitLogSegmentReader.NoOpSegmenter(reader);
      }

   }

   public Iterator<CommitLogSegmentReader.SyncSegment> iterator() {
      return new CommitLogSegmentReader.SegmentIterator();
   }

   private int readSyncMarker(CommitLogDescriptor descriptor, int offset, RandomAccessReader reader) throws IOException {
      if((long)offset > reader.length() - 8L) {
         return -1;
      } else {
         reader.seek((long)offset);
         CRC32 crc = new CRC32();
         FBUtilities.updateChecksumInt(crc, (int)(descriptor.id & 4294967295L));
         FBUtilities.updateChecksumInt(crc, (int)(descriptor.id >>> 32));
         FBUtilities.updateChecksumInt(crc, (int)reader.getPosition());
         int end = reader.readInt();
         long filecrc = (long)reader.readInt() & 4294967295L;
         if(end == 0) {
            if(descriptor.compression != null || descriptor.getEncryptionContext().isEnabled()) {
               return -1;
            }

            end = (int)reader.length();
         } else {
            String msg;
            if(crc.getValue() != filecrc) {
               msg = String.format("Encountered bad header at position %d of commit log %s, with invalid CRC. The end of segment marker should be zero.", new Object[]{Integer.valueOf(offset), reader.getPath()});
               throw new CommitLogSegmentReader.SegmentReadException(msg, true);
            }

            if(end < offset || (long)end > reader.length()) {
               msg = String.format("Encountered bad header at position %d of commit log %s, with bad position but valid CRC", new Object[]{Integer.valueOf(offset), reader.getPath()});
               throw new CommitLogSegmentReader.SegmentReadException(msg, false);
            }
         }

         return end;
      }
   }

   static class EncryptedSegmenter implements CommitLogSegmentReader.Segmenter {
      private final RandomAccessReader reader;
      private final ICompressor compressor;
      private final Cipher cipher;
      private ByteBuffer decryptedBuffer;
      private ByteBuffer uncompressedBuffer;
      private final EncryptedFileSegmentInputStream.ChunkProvider chunkProvider;
      private long currentSegmentEndPosition;
      private long nextLogicalStart;

      public EncryptedSegmenter(CommitLogDescriptor descriptor, RandomAccessReader reader) {
         this(reader, descriptor.getEncryptionContext());
      }

      @VisibleForTesting
      EncryptedSegmenter(RandomAccessReader reader, EncryptionContext encryptionContext) {
         this.reader = reader;
         this.decryptedBuffer = ByteBuffer.allocate(0);
         this.compressor = encryptionContext.getCompressor();
         this.nextLogicalStart = reader.getFilePointer();

         try {
            this.cipher = encryptionContext.getDecryptor();
         } catch (IOException var4) {
            throw new FSReadError(var4, reader.getPath());
         }

         this.chunkProvider = () -> {
            if(reader.getFilePointer() >= this.currentSegmentEndPosition) {
               return ByteBufferUtil.EMPTY_BYTE_BUFFER;
            } else {
               try {
                  this.decryptedBuffer = EncryptionUtils.decrypt((FileDataInput)reader, this.decryptedBuffer, true, this.cipher);
                  this.uncompressedBuffer = EncryptionUtils.uncompress(this.decryptedBuffer, this.uncompressedBuffer, true, this.compressor);
                  return this.uncompressedBuffer;
               } catch (IOException var3) {
                  throw new FSReadError(var3, reader.getPath());
               }
            }
         };
      }

      public CommitLogSegmentReader.SyncSegment nextSegment(int startPosition, int nextSectionStartPosition) throws IOException {
         int totalPlainTextLength = this.reader.readInt();
         this.currentSegmentEndPosition = (long)(nextSectionStartPosition - 1);
         this.nextLogicalStart += 8L;
         FileDataInput input = new EncryptedFileSegmentInputStream(this.reader.getPath(), this.nextLogicalStart, 0, totalPlainTextLength, this.chunkProvider);
         this.nextLogicalStart += (long)totalPlainTextLength;
         return new CommitLogSegmentReader.SyncSegment(input, startPosition, nextSectionStartPosition, (int)this.nextLogicalStart, this.tolerateSegmentErrors(nextSectionStartPosition, this.reader.length()));
      }
   }

   static class CompressedSegmenter implements CommitLogSegmentReader.Segmenter {
      private final ICompressor compressor;
      private final RandomAccessReader reader;
      private byte[] compressedBuffer;
      private byte[] uncompressedBuffer;
      private long nextLogicalStart;

      public CompressedSegmenter(CommitLogDescriptor desc, RandomAccessReader reader) {
         this(CompressionParams.createCompressor(desc.compression), reader);
      }

      public CompressedSegmenter(ICompressor compressor, RandomAccessReader reader) {
         this.compressor = compressor;
         this.reader = reader;
         this.compressedBuffer = new byte[0];
         this.uncompressedBuffer = new byte[0];
         this.nextLogicalStart = reader.getFilePointer();
      }

      public CommitLogSegmentReader.SyncSegment nextSegment(int startPosition, int nextSectionStartPosition) throws IOException {
         this.reader.seek((long)startPosition);
         int uncompressedLength = this.reader.readInt();
         int compressedLength = nextSectionStartPosition - (int)this.reader.getPosition();
         if(compressedLength > this.compressedBuffer.length) {
            this.compressedBuffer = new byte[(int)(1.2D * (double)compressedLength)];
         }

         this.reader.readFully(this.compressedBuffer, 0, compressedLength);
         if(uncompressedLength > this.uncompressedBuffer.length) {
            this.uncompressedBuffer = new byte[(int)(1.2D * (double)uncompressedLength)];
         }

         int count = this.compressor.uncompress(this.compressedBuffer, 0, compressedLength, this.uncompressedBuffer, 0);
         this.nextLogicalStart += 8L;
         FileDataInput input = new FileSegmentInputStream(ByteBuffer.wrap(this.uncompressedBuffer, 0, count), this.reader.getPath(), this.nextLogicalStart);
         this.nextLogicalStart += (long)uncompressedLength;
         return new CommitLogSegmentReader.SyncSegment(input, startPosition, nextSectionStartPosition, (int)this.nextLogicalStart, this.tolerateSegmentErrors(nextSectionStartPosition, this.reader.length()));
      }
   }

   static class NoOpSegmenter implements CommitLogSegmentReader.Segmenter {
      private final RandomAccessReader reader;

      public NoOpSegmenter(RandomAccessReader reader) {
         this.reader = reader;
      }

      public CommitLogSegmentReader.SyncSegment nextSegment(int startPosition, int nextSectionStartPosition) {
         this.reader.seek((long)startPosition);
         return new CommitLogSegmentReader.SyncSegment(this.reader, startPosition, nextSectionStartPosition, nextSectionStartPosition, true);
      }

      public boolean tolerateSegmentErrors(int end, long length) {
         return true;
      }
   }

   interface Segmenter {
      CommitLogSegmentReader.SyncSegment nextSegment(int var1, int var2) throws IOException;

      default boolean tolerateSegmentErrors(int segmentEndPosition, long fileLength) {
         return (long)segmentEndPosition >= fileLength || segmentEndPosition < 0;
      }
   }

   public static class SyncSegment {
      public final FileDataInput input;
      public final int fileStartPosition;
      public final int fileEndPosition;
      public final int endPosition;
      public final boolean toleratesErrorsInSection;

      public SyncSegment(FileDataInput input, int fileStartPosition, int fileEndPosition, int endPosition, boolean toleratesErrorsInSection) {
         this.input = input;
         this.fileStartPosition = fileStartPosition;
         this.fileEndPosition = fileEndPosition;
         this.endPosition = endPosition;
         this.toleratesErrorsInSection = toleratesErrorsInSection;
      }
   }

   public static class SegmentReadException extends IOException {
      public final boolean invalidCrc;

      public SegmentReadException(String msg, boolean invalidCrc) {
         super(msg);
         this.invalidCrc = invalidCrc;
      }
   }

   protected class SegmentIterator extends AbstractIterator<CommitLogSegmentReader.SyncSegment> {
      protected SegmentIterator() {
      }

      protected CommitLogSegmentReader.SyncSegment computeNext() {
         int currentStart;
         do {
            currentStart = CommitLogSegmentReader.this.end;

            try {
               CommitLogSegmentReader.this.end = CommitLogSegmentReader.this.readSyncMarker(CommitLogSegmentReader.this.descriptor, currentStart, CommitLogSegmentReader.this.reader);
               if(CommitLogSegmentReader.this.end == -1) {
                  return (CommitLogSegmentReader.SyncSegment)this.endOfData();
               }

               if((long)CommitLogSegmentReader.this.end > CommitLogSegmentReader.this.reader.length()) {
                  CommitLogSegmentReader.this.end = (int)CommitLogSegmentReader.this.reader.length();
               }

               return CommitLogSegmentReader.this.segmenter.nextSegment(currentStart + 8, CommitLogSegmentReader.this.end);
            } catch (CommitLogSegmentReader.SegmentReadException var4) {
               CommitLogSegmentReader.this.handler.handleUnrecoverableError(new CommitLogReadHandler.CommitLogReadException(var4.getMessage(), CommitLogReadHandler.CommitLogReadErrorReason.UNRECOVERABLE_DESCRIPTOR_ERROR, !var4.invalidCrc && CommitLogSegmentReader.this.tolerateTruncation));
            } catch (IOException var5) {
               boolean tolerateErrorsInSection = CommitLogSegmentReader.this.tolerateTruncation & CommitLogSegmentReader.this.segmenter.tolerateSegmentErrors(CommitLogSegmentReader.this.end, CommitLogSegmentReader.this.reader.length());
               CommitLogSegmentReader.this.handler.handleUnrecoverableError(new CommitLogReadHandler.CommitLogReadException(var5.getMessage(), CommitLogReadHandler.CommitLogReadErrorReason.UNRECOVERABLE_DESCRIPTOR_ERROR, tolerateErrorsInSection));
            }
         } while(currentStart != CommitLogSegmentReader.this.end);

         return (CommitLogSegmentReader.SyncSegment)this.endOfData();
      }
   }
}
