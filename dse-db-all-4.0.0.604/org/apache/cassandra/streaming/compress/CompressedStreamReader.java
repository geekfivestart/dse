package org.apache.cassandra.streaming.compress;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;
import java.util.function.DoubleSupplier;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.util.TrackedInputStream;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamReader;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompressedStreamReader extends StreamReader {
   private static final Logger logger = LoggerFactory.getLogger(CompressedStreamReader.class);
   protected final CompressionInfo compressionInfo;

   public CompressedStreamReader(FileMessageHeader header, StreamSession session) {
      super(header, session);
      this.compressionInfo = header.compressionInfo;
   }

   public SSTableMultiWriter read(ReadableByteChannel channel) throws IOException {
      long totalSize = this.totalSize();
      ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(this.tableId);
      if(cfs == null) {
         throw new IOException("CF " + this.tableId + " was dropped during streaming");
      } else {
         logger.debug("[Stream #{}] Start receiving file #{} from {}, repairedAt = {}, size = {}, ks = '{}', pendingRepair = '{}', table = '{}'.", new Object[]{this.session.planId(), Integer.valueOf(this.fileSeqNum), this.session.peer, Long.valueOf(this.repairedAt), Long.valueOf(totalSize), cfs.keyspace.getName(), this.pendingRepair, cfs.getTableName()});
         InputStream var10002 = Channels.newInputStream(channel);
         CompressionInfo var10003 = this.compressionInfo;
         ChecksumType var10004 = ChecksumType.CRC32;
         cfs.getClass();
         CompressedInputStream cis = new CompressedInputStream(var10002, var10003, var10004, cfs::getCrcCheckChance);
         TrackedInputStream in = new TrackedInputStream(cis);
         StreamReader.StreamDeserializer deserializer = new StreamReader.StreamDeserializer(cfs.metadata(), in, this.inputVersion, this.getHeader(cfs.metadata()));
         SSTableMultiWriter writer = null;

         try {
            writer = this.createWriter(cfs, totalSize, this.repairedAt, this.pendingRepair, this.format);
            String filename = writer.getFilename();
            int sectionIdx = 0;
            Iterator var11 = this.sections.iterator();

            while(var11.hasNext()) {
               Pair<Long, Long> section = (Pair)var11.next();

               assert cis.getTotalCompressedBytesRead() <= totalSize;

               long sectionLength = ((Long)section.right).longValue() - ((Long)section.left).longValue();
               logger.trace("[Stream #{}] Reading section {} with length {} from stream.", new Object[]{this.session.planId(), Integer.valueOf(sectionIdx++), Long.valueOf(sectionLength)});
               cis.position(((Long)section.left).longValue());
               in.reset(0L);

               while(in.getBytesRead() < sectionLength) {
                  this.writePartition(deserializer, writer);
                  this.session.progress(filename, ProgressInfo.Direction.IN, cis.getTotalCompressedBytesRead(), totalSize);
               }
            }

            logger.debug("[Stream #{}] Finished receiving file #{} from {} readBytes = {}, totalSize = {}", new Object[]{this.session.planId(), Integer.valueOf(this.fileSeqNum), this.session.peer, FBUtilities.prettyPrintMemory(cis.getTotalCompressedBytesRead()), FBUtilities.prettyPrintMemory(totalSize)});
            return writer;
         } catch (Throwable var15) {
            logger.warn("[Stream {}] Error while reading partition {} from stream on ks='{}' and table='{}'.", new Object[]{this.session.planId(), deserializer.partitionKey(), cfs.keyspace.getName(), cfs.getTableName()});
            if(writer != null) {
               writer.abort(var15);
            }

            if(Throwables.extractIOExceptionCause(var15).isPresent()) {
               throw var15;
            } else {
               throw com.google.common.base.Throwables.propagate(var15);
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
}
