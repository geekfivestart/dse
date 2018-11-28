package org.apache.cassandra.streaming.compress;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
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
      logger.debug("[Stream #{}] Start streaming file {} to {}, repairedAt = {}, totalSize = {}", new Object[]{this.session.planId(), this.sstable.getFilename(), this.session.peer, this.sstable.getSSTableMetadata().repairedAt, totalSize});
      try (AsynchronousChannelProxy fc = this.sstable.getDataChannel().sharedCopy();){
         long progress = 0L;
         List<Pair<Long, Long>> sections = this.getTransferSections(this.compressionInfo.chunks);
         int sectionIdx = 0;
         for (Pair<Long, Long> section : sections) {
            long lastWrite;
            long length = (Long)section.right - (Long)section.left;
            Object[] arrobject = new Object[]{this.session.planId(), sectionIdx++, length};
            logger.trace("[Stream #{}] Writing section {} with length {} to stream.", arrobject);
            for (long bytesTransferred = 0L; bytesTransferred < length; bytesTransferred += lastWrite) {
               long bytesTransferredFinal = bytesTransferred;
               int toTransfer = (int)Math.min(0xA00000L, length - bytesTransferred);
               this.limiter.acquire(toTransfer);
               lastWrite = out.applyToChannel(wbc -> fc.transferTo((Long)section.left + bytesTransferredFinal, toTransfer, (WritableByteChannel)wbc));
               this.session.progress(this.sstable.descriptor.filenameFor(Component.DATA), ProgressInfo.Direction.OUT, progress += lastWrite, totalSize);
            }
         }
         logger.debug("[Stream #{}] Finished streaming file {} to {}, bytesTransferred = {}, totalSize = {}", new Object[]{this.session.planId(), this.sstable.getFilename(), this.session.peer, FBUtilities.prettyPrintMemory(progress), FBUtilities.prettyPrintMemory(totalSize)});
      }
   }

    protected long totalSize() {
        long size = 0L;
        for (CompressionMetadata.Chunk chunk : this.compressionInfo.chunks) {
            size += (long)(chunk.length + 4);
        }
        return size;
    }

    private List<Pair<Long, Long>> getTransferSections(CompressionMetadata.Chunk[] chunks) {
        ArrayList<Pair<Long, Long>> transferSections = new ArrayList<Pair<Long, Long>>();
        Pair<Long, Long> lastSection = null;
        for (CompressionMetadata.Chunk chunk : chunks) {
            if (lastSection != null) {
                if (chunk.offset == (Long)lastSection.right) {
                    lastSection = Pair.create(lastSection.left, chunk.offset + (long)chunk.length + 4L);
                    continue;
                }
                transferSections.add(lastSection);
                lastSection = Pair.create(chunk.offset, chunk.offset + (long)chunk.length + 4L);
                continue;
            }
            lastSection = Pair.create(chunk.offset, chunk.offset + (long)chunk.length + 4L);
        }
        if (lastSection != null) {
            transferSections.add(lastSection);
        }
        return transferSections;
    }
}
