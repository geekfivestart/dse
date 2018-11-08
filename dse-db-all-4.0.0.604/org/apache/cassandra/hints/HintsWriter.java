package org.apache.cassandra.hints;

import com.google.common.annotations.VisibleForTesting;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.SyncUtil;
import org.apache.cassandra.utils.Throwables;

class HintsWriter implements AutoCloseable {
   static final int PAGE_SIZE = 4096;
   private final File directory;
   private final HintsDescriptor descriptor;
   private final File file;
   protected final FileChannel channel;
   private final int fd;
   protected final CRC32 globalCRC;
   @VisibleForTesting
   AtomicLong totalHintsWritten = new AtomicLong();
   private volatile long lastSyncPosition = 0L;

   protected HintsWriter(File directory, HintsDescriptor descriptor, File file, FileChannel channel, int fd, CRC32 globalCRC) {
      this.directory = directory;
      this.descriptor = descriptor;
      this.file = file;
      this.channel = channel;
      this.fd = fd;
      this.globalCRC = globalCRC;
   }

   static HintsWriter create(File directory, HintsDescriptor descriptor) throws IOException {
      File file = new File(directory, descriptor.fileName());
      FileChannel channel = FileChannel.open(file.toPath(), new OpenOption[]{StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW});
      int fd = NativeLibrary.getfd(channel);
      CRC32 crc = new CRC32();

      try {
         DataOutputBuffer dob = (DataOutputBuffer)DataOutputBuffer.scratchBuffer.get();
         Throwable var7 = null;

         try {
            descriptor.serialize(dob);
            ByteBuffer descriptorBytes = dob.buffer();
            FBUtilities.updateChecksum(crc, descriptorBytes);
            channel.write(descriptorBytes);
         } catch (Throwable var17) {
            var7 = var17;
            throw var17;
         } finally {
            if(dob != null) {
               if(var7 != null) {
                  try {
                     dob.close();
                  } catch (Throwable var16) {
                     var7.addSuppressed(var16);
                  }
               } else {
                  dob.close();
               }
            }

         }
      } catch (Throwable var19) {
         channel.close();
         throw var19;
      }

      return (HintsWriter)(descriptor.isEncrypted()?new EncryptedHintsWriter(directory, descriptor, file, channel, fd, crc):(descriptor.isCompressed()?new CompressedHintsWriter(directory, descriptor, file, channel, fd, crc):new HintsWriter(directory, descriptor, file, channel, fd, crc)));
   }

   HintsDescriptor descriptor() {
      return this.descriptor;
   }

   private void writeStatistics() {
      long hintsWritten = this.totalHintsWritten.get();
      HintsDescriptor.Statistics statistics = new HintsDescriptor.Statistics(hintsWritten);
      this.descriptor.setStatistics(statistics);
      File file = new File(this.directory, this.descriptor.statisticsFileName());

      try {
         DataOutputStream out = new DataOutputStream(Files.newOutputStream(file.toPath(), new OpenOption[0]));
         Throwable var6 = null;

         try {
            statistics.serialize(out);
         } catch (Throwable var16) {
            var6 = var16;
            throw var16;
         } finally {
            if(out != null) {
               if(var6 != null) {
                  try {
                     out.close();
                  } catch (Throwable var15) {
                     var6.addSuppressed(var15);
                  }
               } else {
                  out.close();
               }
            }

         }

      } catch (IOException var18) {
         throw new FSWriteError(var18, file);
      }
   }

   private void writeChecksum() {
      File checksumFile = new File(this.directory, this.descriptor.checksumFileName());

      try {
         OutputStream out = Files.newOutputStream(checksumFile.toPath(), new OpenOption[0]);
         Throwable var3 = null;

         try {
            out.write(Integer.toHexString((int)this.globalCRC.getValue()).getBytes(StandardCharsets.UTF_8));
         } catch (Throwable var13) {
            var3 = var13;
            throw var13;
         } finally {
            if(out != null) {
               if(var3 != null) {
                  try {
                     out.close();
                  } catch (Throwable var12) {
                     var3.addSuppressed(var12);
                  }
               } else {
                  out.close();
               }
            }

         }

      } catch (IOException var15) {
         throw new FSWriteError(var15, checksumFile);
      }
   }

   public void close() {
      File var10000 = this.file;
      Throwables.FileOpType var10001 = Throwables.FileOpType.WRITE;
      Throwables.DiscreteAction[] var10002 = new Throwables.DiscreteAction[]{this::doFsync, null};
      FileChannel var10005 = this.channel;
      this.channel.getClass();
      var10002[1] = var10005::close;
      Throwables.perform(var10000, var10001, var10002);
      this.writeStatistics();
      this.writeChecksum();
   }

   public void fsync() {
      Throwables.perform(this.file, Throwables.FileOpType.WRITE, new Throwables.DiscreteAction[]{this::doFsync});
   }

   private void doFsync() throws IOException {
      SyncUtil.force(this.channel, true);
      this.lastSyncPosition = this.channel.position();
   }

   HintsWriter.Session newSession(ByteBuffer buffer) {
      try {
         return new HintsWriter.Session(buffer, this.channel.size());
      } catch (IOException var3) {
         throw new FSWriteError(var3, this.file);
      }
   }

   @VisibleForTesting
   File getFile() {
      return this.file;
   }

   protected void writeBuffer(ByteBuffer bb) throws IOException {
      FBUtilities.updateChecksum(this.globalCRC, bb);
      this.channel.write(bb);
   }

   final class Session implements AutoCloseable {
      private final ByteBuffer buffer;
      private final long initialSize;
      private long hintsWritten;
      private long bytesWritten;

      Session(ByteBuffer buffer, long initialSize) {
         buffer.clear();
         this.bytesWritten = 0L;
         this.buffer = buffer;
         this.initialSize = initialSize;
      }

      @VisibleForTesting
      long getBytesWritten() {
         return this.bytesWritten;
      }

      long position() {
         return this.initialSize + this.bytesWritten;
      }

      void append(ByteBuffer hint) throws IOException {
         ++this.hintsWritten;
         this.bytesWritten += (long)hint.remaining();
         if(hint.remaining() > this.buffer.remaining()) {
            this.buffer.flip();
            HintsWriter.this.writeBuffer(this.buffer);
            this.buffer.clear();
         }

         if(hint.remaining() <= this.buffer.remaining()) {
            this.buffer.put(hint);
         } else {
            HintsWriter.this.writeBuffer(hint);
         }

      }

      void append(Hint hint) throws IOException {
         int hintSize = (int)((Hint.HintSerializer)Hint.serializers.get(HintsWriter.this.descriptor.version)).serializedSize(hint);
         int totalSize = hintSize + 12;
         if(totalSize > this.buffer.remaining()) {
            this.flushBuffer();
         }

         ByteBuffer hintBuffer = totalSize <= this.buffer.remaining()?this.buffer:ByteBuffer.allocate(totalSize);
         CRC32 crc = new CRC32();
         DataOutputBufferFixed out = new DataOutputBufferFixed(hintBuffer);
         Throwable var7 = null;

         try {
            out.writeInt(hintSize);
            FBUtilities.updateChecksumInt(crc, hintSize);
            out.writeInt((int)crc.getValue());
            ((Hint.HintSerializer)Hint.serializers.get(HintsWriter.this.descriptor.version)).serialize((Hint)hint, out);
            FBUtilities.updateChecksum(crc, hintBuffer, hintBuffer.position() - hintSize, hintSize);
            out.writeInt((int)crc.getValue());
         } catch (Throwable var16) {
            var7 = var16;
            throw var16;
         } finally {
            if(out != null) {
               if(var7 != null) {
                  try {
                     out.close();
                  } catch (Throwable var15) {
                     var7.addSuppressed(var15);
                  }
               } else {
                  out.close();
               }
            }

         }

         if(hintBuffer == this.buffer) {
            this.bytesWritten += (long)totalSize;
         } else {
            this.append((ByteBuffer)hintBuffer.flip());
         }

      }

      public void close() throws IOException {
         this.flushBuffer();
         this.maybeFsync();
         this.maybeSkipCache();
         HintsWriter.this.totalHintsWritten.addAndGet(this.hintsWritten);
         StorageMetrics.hintsOnDisk.inc(this.hintsWritten);
      }

      private void flushBuffer() throws IOException {
         this.buffer.flip();
         if(this.buffer.remaining() > 0) {
            HintsWriter.this.writeBuffer(this.buffer);
         }

         this.buffer.clear();
      }

      private void maybeFsync() {
         if(this.position() >= HintsWriter.this.lastSyncPosition + (long)DatabaseDescriptor.getTrickleFsyncIntervalInKb() * 1024L) {
            HintsWriter.this.fsync();
         }

      }

      private void maybeSkipCache() {
         long position = this.position();
         if(position >= (long)DatabaseDescriptor.getTrickleFsyncIntervalInKb() * 1024L) {
            NativeLibrary.trySkipCache(HintsWriter.this.fd, 0L, position - position % 4096L, HintsWriter.this.file.getPath());
         }

      }
   }
}
