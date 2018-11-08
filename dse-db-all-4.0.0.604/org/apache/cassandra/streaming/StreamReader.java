package org.apache.cassandra.streaming;

import com.google.common.base.Throwables;
import com.google.common.collect.UnmodifiableIterator;
import com.ning.compress.lzf.LZFInputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.SSTableSimpleIterator;
import org.apache.cassandra.io.sstable.format.RangeAwareSSTableWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.TrackedInputStream;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamReader {
   private static final Logger logger = LoggerFactory.getLogger(StreamReader.class);
   protected final TableId tableId;
   protected final long estimatedKeys;
   protected final Collection<Pair<Long, Long>> sections;
   protected final StreamSession session;
   protected final Version inputVersion;
   protected final long repairedAt;
   protected final UUID pendingRepair;
   protected final SSTableFormat.Type format;
   protected final int sstableLevel;
   protected final SerializationHeader.Component header;
   protected final int fileSeqNum;

   public StreamReader(FileMessageHeader header, StreamSession session) {
      assert session.getPendingRepair() == null || session.getPendingRepair().equals(header.pendingRepair);

      this.session = session;
      this.tableId = header.tableId;
      this.estimatedKeys = header.estimatedKeys;
      this.sections = header.sections;
      this.inputVersion = header.version;
      this.repairedAt = header.repairedAt;
      this.pendingRepair = header.pendingRepair;
      this.format = header.format;
      this.sstableLevel = header.sstableLevel;
      this.header = header.header;
      this.fileSeqNum = header.sequenceNumber;
   }

   public SSTableMultiWriter read(ReadableByteChannel channel) throws IOException {
      long totalSize = this.totalSize();
      ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(this.tableId);
      if(cfs == null) {
         throw new IOException("CF " + this.tableId + " was dropped during streaming");
      } else {
         logger.debug("[Stream #{}] Start receiving file #{} from {}, repairedAt = {}, size = {}, ks = '{}', table = '{}', pendingRepair = '{}'.", new Object[]{this.session.planId(), Integer.valueOf(this.fileSeqNum), this.session.peer, Long.valueOf(this.repairedAt), Long.valueOf(totalSize), cfs.keyspace.getName(), cfs.getTableName(), this.pendingRepair});
         TrackedInputStream in = new TrackedInputStream(new LZFInputStream(Channels.newInputStream(channel)));
         StreamReader.StreamDeserializer deserializer = new StreamReader.StreamDeserializer(cfs.metadata(), in, this.inputVersion, this.getHeader(cfs.metadata()));
         SSTableMultiWriter writer = null;

         try {
            writer = this.createWriter(cfs, totalSize, this.repairedAt, this.pendingRepair, this.format);

            while(in.getBytesRead() < totalSize) {
               this.writePartition(deserializer, writer);
               this.session.progress(writer.getFilename(), ProgressInfo.Direction.IN, in.getBytesRead(), totalSize);
            }

            logger.debug("[Stream #{}] Finished receiving file #{} from {} readBytes = {}, totalSize = {}", new Object[]{this.session.planId(), Integer.valueOf(this.fileSeqNum), this.session.peer, FBUtilities.prettyPrintMemory(in.getBytesRead()), FBUtilities.prettyPrintMemory(totalSize)});
            return writer;
         } catch (Throwable var9) {
            logger.warn("[Stream {}] Error while reading partition {} from stream on ks='{}' and table='{}'.", new Object[]{this.session.planId(), deserializer.partitionKey(), cfs.keyspace.getName(), cfs.getTableName(), var9});
            if(writer != null) {
               writer.abort(var9);
            }

            throw Throwables.propagate(var9);
         }
      }
   }

   protected SerializationHeader getHeader(TableMetadata metadata) {
      return this.header != null?this.header.toHeader(metadata):null;
   }

   protected SSTableMultiWriter createWriter(ColumnFamilyStore cfs, long totalSize, long repairedAt, UUID pendingRepair, SSTableFormat.Type format) throws IOException {
      Directories.DataDirectory localDir = cfs.getDirectories().getWriteableLocation(totalSize);
      if(localDir == null) {
         throw new IOException(String.format("Insufficient disk space to store %s", new Object[]{FBUtilities.prettyPrintMemory(totalSize)}));
      } else {
         SSTableFormat.Type writeFormat = SSTableFormat.streamWriteFormat();
         RangeAwareSSTableWriter writer = new RangeAwareSSTableWriter(cfs, this.estimatedKeys, repairedAt, pendingRepair, writeFormat, this.sstableLevel, totalSize, this.session.getTransaction(this.tableId), this.getHeader(cfs.metadata()));
         StreamHook.instance.reportIncomingFile(cfs, writer, this.session, this.fileSeqNum);
         return writer;
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

   protected void writePartition(StreamReader.StreamDeserializer deserializer, SSTableMultiWriter writer) throws IOException {
      writer.append(deserializer.newPartition());
      deserializer.checkForExceptions();
   }

   public static class StreamDeserializer extends UnmodifiableIterator<Unfiltered> implements UnfilteredRowIterator {
      private final TableMetadata metadata;
      private final DataInputPlus in;
      private final SerializationHeader header;
      private final SerializationHelper helper;
      private DecoratedKey key;
      private DeletionTime partitionLevelDeletion;
      private SSTableSimpleIterator iterator;
      private Row staticRow;
      private IOException exception;

      public StreamDeserializer(TableMetadata metadata, InputStream in, Version version, SerializationHeader header) throws IOException {
         this.metadata = metadata;
         this.in = new DataInputPlus.DataInputStreamPlus(in);
         this.helper = new SerializationHelper(metadata, version.encodingVersion(), SerializationHelper.Flag.PRESERVE_SIZE);
         this.header = header;
      }

      public StreamReader.StreamDeserializer newPartition() throws IOException {
         this.key = this.metadata.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(this.in));
         this.partitionLevelDeletion = DeletionTime.serializer.deserialize(this.in);
         this.iterator = SSTableSimpleIterator.create(this.metadata, this.in, this.header, this.helper);
         this.staticRow = this.iterator.readStaticRow();
         return this;
      }

      public TableMetadata metadata() {
         return this.metadata;
      }

      public RegularAndStaticColumns columns() {
         return this.metadata.regularAndStaticColumns();
      }

      public boolean isReverseOrder() {
         return false;
      }

      public DecoratedKey partitionKey() {
         return this.key;
      }

      public DeletionTime partitionLevelDeletion() {
         return this.partitionLevelDeletion;
      }

      public Row staticRow() {
         return this.staticRow;
      }

      public EncodingStats stats() {
         return this.header.stats();
      }

      public boolean hasNext() {
         try {
            return this.iterator.hasNext();
         } catch (IOError var2) {
            if(var2.getCause() != null && var2.getCause() instanceof IOException) {
               this.exception = (IOException)var2.getCause();
               return false;
            } else {
               throw var2;
            }
         }
      }

      public Unfiltered next() {
         Unfiltered unfiltered = (Unfiltered)this.iterator.next();
         return (Unfiltered)(this.metadata.isCounter() && unfiltered.kind() == Unfiltered.Kind.ROW?this.maybeMarkLocalToBeCleared((Row)unfiltered):unfiltered);
      }

      private Row maybeMarkLocalToBeCleared(Row row) {
         return this.metadata.isCounter()?row.markCounterLocalToBeCleared():row;
      }

      public void checkForExceptions() throws IOException {
         if(this.exception != null) {
            throw this.exception;
         }
      }

      public void close() {
      }
   }
}
