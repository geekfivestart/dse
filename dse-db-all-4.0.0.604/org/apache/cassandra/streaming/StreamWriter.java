package org.apache.cassandra.streaming;

import com.ning.compress.lzf.LZFOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Iterator;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamWriter {
   private static final int DEFAULT_CHUNK_SIZE = 65536;
   private static final Logger logger = LoggerFactory.getLogger(StreamWriter.class);
   protected final SSTableReader sstable;
   protected final Collection<Pair<Long, Long>> sections;
   protected final StreamManager.StreamRateLimiter limiter;
   protected final StreamSession session;
   private OutputStream compressedOutput;
   private byte[] transferBuffer;

   public StreamWriter(SSTableReader sstable, Collection<Pair<Long, Long>> sections, StreamSession session) {
      this.session = session;
      this.sstable = sstable;
      this.sections = sections;
      this.limiter = StreamManager.getRateLimiter(session.peer);
   }

   public void write(DataOutputStreamPlus output) throws IOException {
      long totalSize = this.totalSize();
      logger.debug("[Stream #{}] Start streaming file {} to {}, repairedAt = {}, totalSize = {}", new Object[]{this.session.planId(), this.sstable.getFilename(), this.session.peer, Long.valueOf(this.sstable.getSSTableMetadata().repairedAt), Long.valueOf(totalSize)});
      RandomAccessReader file = this.sstable.openDataReader();
      Throwable var5 = null;

      try {
         DataIntegrityMetadata.ChecksumValidator validator = (new File(this.sstable.descriptor.filenameFor(Component.CRC))).exists()?DataIntegrityMetadata.checksumValidator(this.sstable.descriptor):null;
         Throwable var7 = null;

         try {
            this.transferBuffer = validator == null?new byte[65536]:new byte[validator.chunkSize];
            this.compressedOutput = new LZFOutputStream(output);
            long progress = 0L;
            Iterator var10 = this.sections.iterator();

            while(var10.hasNext()) {
               Pair<Long, Long> section = (Pair)var10.next();
               long start = validator == null?((Long)section.left).longValue():validator.chunkStart(((Long)section.left).longValue());
               int readOffset = (int)(((Long)section.left).longValue() - start);
               file.seek(start);
               if(validator != null) {
                  validator.seek(start);
               }

               long length = ((Long)section.right).longValue() - start;

               for(long bytesRead = 0L; bytesRead < length; readOffset = 0) {
                  long lastBytesRead = this.write(file, validator, readOffset, length, bytesRead);
                  bytesRead += lastBytesRead;
                  progress += lastBytesRead - (long)readOffset;
                  this.session.progress(this.sstable.descriptor.filenameFor(Component.DATA), ProgressInfo.Direction.OUT, progress, totalSize);
               }

               this.compressedOutput.flush();
            }

            logger.debug("[Stream #{}] Finished streaming file {} to {}, bytesTransferred = {}, totalSize = {}", new Object[]{this.session.planId(), this.sstable.getFilename(), this.session.peer, FBUtilities.prettyPrintMemory(progress), FBUtilities.prettyPrintMemory(totalSize)});
         } catch (Throwable var42) {
            var7 = var42;
            throw var42;
         } finally {
            if(validator != null) {
               if(var7 != null) {
                  try {
                     validator.close();
                  } catch (Throwable var41) {
                     var7.addSuppressed(var41);
                  }
               } else {
                  validator.close();
               }
            }

         }
      } catch (Throwable var44) {
         var5 = var44;
         throw var44;
      } finally {
         if(file != null) {
            if(var5 != null) {
               try {
                  file.close();
               } catch (Throwable var40) {
                  var5.addSuppressed(var40);
               }
            } else {
               file.close();
            }
         }

      }
   }

   protected long totalSize() {
      long size = 0L;

      Pair section;
      for(Iterator var3 = this.sections.iterator(); var3.hasNext(); size += ((Long)section.right).longValue() - ((Long)section.left).longValue()) {
         section = (Pair)var3.next();
      }

      return size;
   }

   protected long write(RandomAccessReader reader, DataIntegrityMetadata.ChecksumValidator validator, int start, long length, long bytesTransferred) throws IOException {
      int toTransfer = (int)Math.min((long)this.transferBuffer.length, length - bytesTransferred);
      int minReadable = (int)Math.min((long)this.transferBuffer.length, reader.length() - reader.getFilePointer());
      reader.readFully(this.transferBuffer, 0, minReadable);
      if(validator != null) {
         validator.validate(this.transferBuffer, 0, minReadable);
      }

      this.limiter.acquire(toTransfer - start);
      this.compressedOutput.write(this.transferBuffer, start, toTransfer - start);
      return (long)toTransfer;
   }
}
