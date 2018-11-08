package org.apache.cassandra.streaming.compress;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.AsynchronousChannelProxy;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.ThrowingFunction;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamWriter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompressedStreamWriter extends StreamWriter {
   public static final int CHUNK_SIZE = 10485760;
   private static final Logger logger = LoggerFactory.getLogger(CompressedStreamWriter.class);
   private final CompressionInfo compressionInfo;

   public CompressedStreamWriter(SSTableReader sstable, Collection<Pair<Long, Long>> sections, CompressionInfo compressionInfo, StreamSession session) {
      super(sstable, sections, session);
      this.compressionInfo = compressionInfo;
   }

   public void write(DataOutputStreamPlus out) throws IOException {
      long totalSize = this.totalSize();
      logger.debug("[Stream #{}] Start streaming file {} to {}, repairedAt = {}, totalSize = {}", new Object[]{this.session.planId(), this.sstable.getFilename(), this.session.peer, Long.valueOf(this.sstable.getSSTableMetadata().repairedAt), Long.valueOf(totalSize)});
      AsynchronousChannelProxy fc = this.sstable.getDataChannel().sharedCopy();
      Throwable var5 = null;

      try {
         long progress = 0L;
         List<Pair<Long, Long>> sections = this.getTransferSections(this.compressionInfo.chunks);
         int sectionIdx = 0;
         Iterator var10 = sections.iterator();

         label89:
         while(true) {
            if(var10.hasNext()) {
               Pair<Long, Long> section = (Pair)var10.next();
               long length = ((Long)section.right).longValue() - ((Long)section.left).longValue();
               logger.trace("[Stream #{}] Writing section {} with length {} to stream.", new Object[]{this.session.planId(), Integer.valueOf(sectionIdx++), Long.valueOf(length)});
               long bytesTransferred = 0L;

               while(true) {
                  if(bytesTransferred >= length) {
                     continue label89;
                  }

                  int toTransfer = (int)Math.min(10485760L, length - bytesTransferred);
                  this.limiter.acquire(toTransfer);
                  long lastWrite = ((Long)out.applyToChannel((wbc) -> {
                     return Long.valueOf(fc.transferTo(((Long)section.left).longValue() + bytesTransferred, (long)toTransfer, wbc));
                  })).longValue();
                  bytesTransferred += lastWrite;
                  progress += lastWrite;
                  this.session.progress(this.sstable.descriptor.filenameFor(Component.DATA), ProgressInfo.Direction.OUT, progress, totalSize);
               }
            }

            logger.debug("[Stream #{}] Finished streaming file {} to {}, bytesTransferred = {}, totalSize = {}", new Object[]{this.session.planId(), this.sstable.getFilename(), this.session.peer, FBUtilities.prettyPrintMemory(progress), FBUtilities.prettyPrintMemory(totalSize)});
            return;
         }
      } catch (Throwable var28) {
         var5 = var28;
         throw var28;
      } finally {
         if(fc != null) {
            if(var5 != null) {
               try {
                  fc.close();
               } catch (Throwable var27) {
                  var5.addSuppressed(var27);
               }
            } else {
               fc.close();
            }
         }

      }
   }

   protected long totalSize() {
      long size = 0L;
      CompressionMetadata.Chunk[] var3 = this.compressionInfo.chunks;
      int var4 = var3.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         CompressionMetadata.Chunk chunk = var3[var5];
         size += (long)(chunk.length + 4);
      }

      return size;
   }

   private List<Pair<Long, Long>> getTransferSections(CompressionMetadata.Chunk[] chunks) {
      List<Pair<Long, Long>> transferSections = new ArrayList();
      Pair<Long, Long> lastSection = null;
      CompressionMetadata.Chunk[] var4 = chunks;
      int var5 = chunks.length;

      for(int var6 = 0; var6 < var5; ++var6) {
         CompressionMetadata.Chunk chunk = var4[var6];
         if(lastSection != null) {
            if(chunk.offset == ((Long)lastSection.right).longValue()) {
               lastSection = Pair.create(lastSection.left, Long.valueOf(chunk.offset + (long)chunk.length + 4L));
            } else {
               transferSections.add(lastSection);
               lastSection = Pair.create(Long.valueOf(chunk.offset), Long.valueOf(chunk.offset + (long)chunk.length + 4L));
            }
         } else {
            lastSection = Pair.create(Long.valueOf(chunk.offset), Long.valueOf(chunk.offset + (long)chunk.length + 4L));
         }
      }

      if(lastSection != null) {
         transferSections.add(lastSection);
      }

      return transferSections;
   }
}
