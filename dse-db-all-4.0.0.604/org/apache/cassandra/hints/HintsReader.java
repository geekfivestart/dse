package org.apache.cassandra.hints;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.RateLimiter;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import javax.annotation.Nullable;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HintsReader implements AutoCloseable, Iterable<HintsReader.Page> {
   private static final Logger logger = LoggerFactory.getLogger(HintsReader.class);
   private static final int PAGE_SIZE = 524288;
   private final HintsDescriptor descriptor;
   private final File file;
   private final ChecksummedDataInput input;
   @Nullable
   private final RateLimiter rateLimiter;

   protected HintsReader(HintsDescriptor descriptor, File file, ChecksummedDataInput reader, RateLimiter rateLimiter) {
      this.descriptor = descriptor;
      this.file = file;
      this.input = reader;
      this.rateLimiter = rateLimiter;
   }

   static HintsReader open(File file, RateLimiter rateLimiter) {
      ChecksummedDataInput reader = ChecksummedDataInput.open(file);

      try {
         HintsDescriptor descriptor = HintsDescriptor.deserialize(reader);
         descriptor.loadStatsComponent(file.getParent());
         if(descriptor.isCompressed()) {
            reader = CompressedChecksummedDataInput.upgradeInput(reader, descriptor.createCompressor());
         } else if(descriptor.isEncrypted()) {
            reader = EncryptedChecksummedDataInput.upgradeInput(reader, descriptor.getCipher(), descriptor.createCompressor());
         }

         return new HintsReader(descriptor, file, reader, rateLimiter);
      } catch (IOException var4) {
         reader.close();
         throw new FSReadError(var4, file);
      }
   }

   static HintsReader open(File file) {
      return open(file, (RateLimiter)null);
   }

   public void close() {
      this.input.close();
   }

   public HintsDescriptor descriptor() {
      return this.descriptor;
   }

   void seek(InputPosition newPosition) {
      this.input.seek(newPosition);
   }

   public Iterator<HintsReader.Page> iterator() {
      return new HintsReader.PagesIterator();
   }

   public ChecksummedDataInput getInput() {
      return this.input;
   }

   final class BuffersIterator extends AbstractIterator<ByteBuffer> {
      private final InputPosition offset;
      private final long now = System.currentTimeMillis();

      BuffersIterator(InputPosition offset) {
         this.offset = offset;
      }

      protected ByteBuffer computeNext() {
         ByteBuffer buffer;
         do {
            InputPosition position = HintsReader.this.input.getSeekPosition();
            if(HintsReader.this.input.isEOF()) {
               return (ByteBuffer)this.endOfData();
            }

            if(position.subtract(this.offset) >= 524288L) {
               return (ByteBuffer)this.endOfData();
            }

            try {
               buffer = this.computeNextInternal();
            } catch (EOFException var4) {
               HintsReader.logger.warn("Unexpected EOF replaying hints ({}), likely due to unflushed hint file on shutdown; continuing", HintsReader.this.descriptor.fileName(), var4);
               return (ByteBuffer)this.endOfData();
            } catch (IOException var5) {
               throw new FSReadError(var5, HintsReader.this.file);
            }
         } while(buffer == null);

         return buffer;
      }

      private ByteBuffer computeNextInternal() throws IOException {
         HintsReader.this.input.resetCrc();
         HintsReader.this.input.resetLimit();
         int size = HintsReader.this.input.readInt();
         if(!HintsReader.this.input.checkCrc()) {
            throw new IOException("Digest mismatch exception");
         } else {
            return this.readBuffer(size);
         }
      }

      private ByteBuffer readBuffer(int size) throws IOException {
         if(HintsReader.this.rateLimiter != null) {
            HintsReader.this.rateLimiter.acquire(size);
         }

         HintsReader.this.input.limit((long)size);
         ByteBuffer buffer = ((Hint.HintSerializer)Hint.serializers.get(HintsReader.this.descriptor.version)).readBufferIfLive(HintsReader.this.input, this.now, size);
         if(HintsReader.this.input.checkCrc()) {
            return buffer;
         } else {
            HintsReader.logger.warn("Failed to read a hint for {} - digest mismatch for hint at position {} in file {}", new Object[]{HintsReader.this.descriptor.hostId, Long.valueOf(HintsReader.this.input.getPosition() - (long)size - 4L), HintsReader.this.descriptor.fileName()});
            return null;
         }
      }
   }

   final class HintsIterator extends AbstractIterator<Hint> {
      private final InputPosition offset;
      private final long now = System.currentTimeMillis();

      HintsIterator(InputPosition offset) {
         this.offset = offset;
      }

      protected Hint computeNext() {
         Hint hint;
         do {
            InputPosition position = HintsReader.this.input.getSeekPosition();
            if(HintsReader.this.input.isEOF()) {
               return (Hint)this.endOfData();
            }

            if(position.subtract(this.offset) >= 524288L) {
               return (Hint)this.endOfData();
            }

            try {
               hint = this.computeNextInternal();
            } catch (EOFException var4) {
               HintsReader.logger.warn("Unexpected EOF replaying hints ({}), likely due to unflushed hint file on shutdown; continuing", HintsReader.this.descriptor.fileName(), var4);
               return (Hint)this.endOfData();
            } catch (IOException var5) {
               throw new FSReadError(var5, HintsReader.this.file);
            }
         } while(hint == null);

         return hint;
      }

      private Hint computeNextInternal() throws IOException {
         HintsReader.this.input.resetCrc();
         HintsReader.this.input.resetLimit();
         int size = HintsReader.this.input.readInt();
         if(!HintsReader.this.input.checkCrc()) {
            throw new IOException("Digest mismatch exception");
         } else {
            return this.readHint(size);
         }
      }

      private Hint readHint(int size) throws IOException {
         if(HintsReader.this.rateLimiter != null) {
            HintsReader.this.rateLimiter.acquire(size);
         }

         HintsReader.this.input.limit((long)size);

         Hint hint;
         try {
            hint = ((Hint.HintSerializer)Hint.serializers.get(HintsReader.this.descriptor.version)).deserializeIfLive(HintsReader.this.input, this.now, (long)size);
            HintsReader.this.input.checkLimit(0);
         } catch (UnknownTableException var4) {
            HintsReader.logger.warn("Failed to read a hint for {}: {} - table with id {} is unknown in file {}", new Object[]{StorageService.instance.getEndpointForHostId(HintsReader.this.descriptor.hostId), HintsReader.this.descriptor.hostId, var4.id, HintsReader.this.descriptor.fileName()});
            HintsReader.this.input.skipBytes(Ints.checkedCast((long)size - HintsReader.this.input.bytesPastLimit()));
            hint = null;
         }

         if(HintsReader.this.input.checkCrc()) {
            return hint;
         } else {
            HintsReader.logger.warn("Failed to read a hint for {}: {} - digest mismatch for hint at position {} in file {}", new Object[]{StorageService.instance.getEndpointForHostId(HintsReader.this.descriptor.hostId), HintsReader.this.descriptor.hostId, Long.valueOf(HintsReader.this.input.getPosition() - (long)size - 4L), HintsReader.this.descriptor.fileName()});
            return null;
         }
      }
   }

   final class PagesIterator extends AbstractIterator<HintsReader.Page> {
      PagesIterator() {
      }

      protected HintsReader.Page computeNext() {
         HintsReader.this.input.tryUncacheRead();
         return HintsReader.this.input.isEOF()?(HintsReader.Page)this.endOfData():HintsReader.this.new Page(HintsReader.this.input.getSeekPosition());
      }
   }

   final class Page {
      public final InputPosition position;

      private Page(InputPosition inputPosition) {
         this.position = inputPosition;
      }

      Iterator<Hint> hintsIterator() {
         return HintsReader.this.new HintsIterator(this.position);
      }

      Iterator<ByteBuffer> buffersIterator() {
         return HintsReader.this.new BuffersIterator(this.position);
      }
   }
}
