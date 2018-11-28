package org.apache.cassandra.io.sstable.format.trieindex;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.ChecksummedSequentialWriter;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.SyncUtil;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@VisibleForTesting
public class TrieIndexSSTableWriter extends SSTableWriter {
   private static final Logger logger = LoggerFactory.getLogger(TrieIndexSSTableWriter.class);
   private final PartitionWriter partitionWriter;
   private final TrieIndexSSTableWriter.IndexWriter iwriter;
   private final FileHandle.Builder dbuilder;
   protected final SequentialWriter dataFile;
   private DecoratedKey lastWrittenKey;
   private DataPosition dataMark;
   private long lastEarlyOpenLength = 0L;
   private final Optional<ChunkCache> chunkCache = Optional.ofNullable(ChunkCache.instance);
   private static final SequentialWriterOption WRITER_OPTION;


   public TrieIndexSSTableWriter(Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, TableMetadataRef metadata, MetadataCollector metadataCollector, SerializationHeader header, Collection<SSTableFlushObserver> observers, LifecycleTransaction txn) {
      super(descriptor, keyCount, repairedAt, pendingRepair, metadata, metadataCollector, header, observers);
      txn.trackNew(this);
      this.dataFile = this.compression ? new CompressedSequentialWriter(new File(this.getFilename()), descriptor.filenameFor(Component.COMPRESSION_INFO), new File(descriptor.filenameFor(Component.DIGEST)), WRITER_OPTION, this.metadata().params.compression, metadataCollector) : new ChecksummedSequentialWriter(new File(this.getFilename()), new File(descriptor.filenameFor(Component.CRC)), new File(descriptor.filenameFor(Component.DIGEST)), WRITER_OPTION);
      this.dbuilder = new FileHandle.Builder(descriptor.filenameFor(Component.DATA)).compressed(this.compression).mmapped(metadata.get().diskAccessMode == Config.AccessMode.mmap);
      this.chunkCache.ifPresent(this.dbuilder::withChunkCache);
      this.iwriter = new IndexWriter(keyCount);
      this.partitionWriter = new PartitionWriter(this.header, this.metadata().comparator, this.dataFile, this.iwriter.rowIndexFile, descriptor.version, this.observers);
   }

   public void mark() {
      this.dataMark = this.dataFile.mark();
      this.iwriter.mark();
   }

   public void resetAndTruncate() {
      this.dataFile.resetAndTruncate(this.dataMark);
      this.iwriter.resetAndTruncate();
   }

   protected long beforeAppend(DecoratedKey decoratedKey) {
      assert decoratedKey != null : "Keys must not be null";

      if(this.lastWrittenKey != null && this.lastWrittenKey.compareTo((PartitionPosition)decoratedKey) >= 0) {
         throw new RuntimeException("Last written key " + this.lastWrittenKey + " >= current key " + decoratedKey + " writing into " + this.getFilename());
      } else {
         return this.lastWrittenKey == null?0L:this.dataFile.position();
      }
   }

   private long afterAppend(DecoratedKey decoratedKey, RowIndexEntry index) throws IOException {
      this.metadataCollector.addKey(decoratedKey.getKey());
      this.lastWrittenKey = decoratedKey;
      this.last = this.lastWrittenKey;
      if(this.first == null) {
         this.first = this.lastWrittenKey;
      }

      if(logger.isTraceEnabled()) {
         logger.trace("wrote {} at {}", decoratedKey, Long.valueOf(index.position));
      }

      return this.iwriter.append(decoratedKey, index);
   }

   public RowIndexEntry append(UnfilteredRowIterator iterator) {
      DecoratedKey key = iterator.partitionKey();
      if(key.getKey().remaining() > '\uffff') {
         logger.error("Key size {} exceeds maximum of {}, skipping row", Integer.valueOf(key.getKey().remaining()), Integer.valueOf('\uffff'));
         return null;
      } else if(iterator.isEmpty()) {
         return null;
      } else {
         long startPosition = this.beforeAppend(key);
         this.observers.forEach((o) -> {
            o.startPartition(key, startPosition);
         });
         this.partitionWriter.reset();

         try {
            UnfilteredRowIterator collecting = Transformation.apply((UnfilteredRowIterator)iterator, new TrieIndexSSTableWriter.StatsCollector(this.metadataCollector));
            Throwable var6 = null;

            RowIndexEntry var14;
            try {
               long trieRoot = this.partitionWriter.writePartition(collecting);
               RowIndexEntry entry = TrieIndexEntry.create(startPosition, trieRoot, collecting.partitionLevelDeletion(), this.partitionWriter.rowIndexCount);
               long endPosition = this.dataFile.position();
               long rowSize = endPosition - startPosition;
               this.maybeLogLargePartitionWarning(key, rowSize);
               this.metadataCollector.addPartitionSizeInBytes(rowSize);
               this.afterAppend(key, entry);
               var14 = entry;
            } catch (Throwable var24) {
               var6 = var24;
               throw var24;
            } finally {
               if(collecting != null) {
                  if(var6 != null) {
                     try {
                        collecting.close();
                     } catch (Throwable var23) {
                        var6.addSuppressed(var23);
                     }
                  } else {
                     collecting.close();
                  }
               }

            }

            return var14;
         } catch (IOException var26) {
            throw new FSWriteError(var26, this.dataFile.getPath());
         }
      }
   }

   private void maybeLogLargePartitionWarning(DecoratedKey key, long rowSize) {
      if(rowSize > DatabaseDescriptor.getCompactionLargePartitionWarningThreshold()) {
         String keyString = this.metadata().partitionKeyType.getString(key.getKey());
         logger.warn("Writing large partition {}/{}:{} ({}) to sstable {}", new Object[]{this.metadata.keyspace, this.metadata.name, keyString, FBUtilities.prettyPrintMemory(rowSize), this.getFilename()});
      }

   }

   public boolean openEarly(Consumer<SSTableReader> callWhenReady) {
      long dataLength = this.dataFile.position();
      this.dataFile.requestSyncOnNextFlush();
      this.iwriter.rowIndexFile.requestSyncOnNextFlush();
      this.iwriter.partitionIndexFile.requestSyncOnNextFlush();
      return this.iwriter.buildPartial(dataLength, (partitionIndex) -> {
         StatsMetadata stats = this.statsMetadata();
         FileHandle ifile = this.iwriter.rowIndexFHBuilder.complete();
         if(this.compression) {
            this.dbuilder.withCompressionMetadata(((CompressedSequentialWriter)this.dataFile).open(dataLength));
         }

         int dataBufferSize = this.optimizationStrategy.bufferSize(stats.estimatedPartitionSize.percentile(DatabaseDescriptor.getDiskOptimizationEstimatePercentile()));
         FileHandle dfile = this.dbuilder.bufferSize(dataBufferSize).complete(dataLength);
         this.invalidateCacheAtBoundary(dfile);
         SSTableReader sstable = TrieIndexSSTableReader.internalOpen(this.descriptor, this.components, this.metadata, ifile, dfile, partitionIndex, this.iwriter.bf.sharedCopy(), this.maxDataAge, stats, SSTableReader.OpenReason.EARLY, this.header);
         sstable.first = getMinimalKey(partitionIndex.firstKey());
         sstable.last = getMinimalKey(partitionIndex.lastKey());
         callWhenReady.accept(sstable);
      });
   }

   void invalidateCacheAtBoundary(FileHandle dfile) {
      this.chunkCache.ifPresent((cache) -> {
         if(this.lastEarlyOpenLength != 0L && dfile.dataLength() > this.lastEarlyOpenLength) {
            ChunkCache.invalidatePosition(dfile, this.lastEarlyOpenLength);
         }

      });
      this.lastEarlyOpenLength = dfile.dataLength();
   }

   public SSTableReader openFinalEarly() {
      this.dataFile.sync();
      this.iwriter.rowIndexFile.sync();
      this.iwriter.partitionIndexFile.sync();
      return this.openFinal(SSTableReader.OpenReason.EARLY);
   }

   private SSTableReader openFinal(SSTableReader.OpenReason openReason) {
      if(this.maxDataAge < 0L) {
         this.maxDataAge = System.currentTimeMillis();
      }

      StatsMetadata stats = this.statsMetadata();
      PartitionIndex partitionIndex = this.iwriter.completedPartitionIndex();
      FileHandle rowIndexFile = this.iwriter.rowIndexFHBuilder.complete();
      int dataBufferSize = this.optimizationStrategy.bufferSize(stats.estimatedPartitionSize.percentile(DatabaseDescriptor.getDiskOptimizationEstimatePercentile()));
      if(this.compression) {
         this.dbuilder.withCompressionMetadata(((CompressedSequentialWriter)this.dataFile).open(0L));
      }

      FileHandle dfile = this.dbuilder.bufferSize(dataBufferSize).complete();
      this.invalidateCacheAtBoundary(dfile);
      SSTableReader sstable = TrieIndexSSTableReader.internalOpen(this.descriptor, this.components, this.metadata, rowIndexFile, dfile, partitionIndex, this.iwriter.bf.sharedCopy(), this.maxDataAge, stats, openReason, this.header);
      sstable.first = getMinimalKey(this.first);
      sstable.last = getMinimalKey(this.last);
      return sstable;
   }

   protected SSTableWriter.TransactionalProxy txnProxy() {
      return new TrieIndexSSTableWriter.TransactionalProxy();
   }

   private void writeMetadata(Descriptor desc, Map<MetadataType, MetadataComponent> components) {
      File file = new File(desc.filenameFor(Component.STATS));

      try {
         SequentialWriter out = new SequentialWriter(file, WRITER_OPTION);
         Throwable var5 = null;

         try {
            desc.getMetadataSerializer().serialize(components, out, desc.version);
            out.finish();
         } catch (Throwable var15) {
            var5 = var15;
            throw var15;
         } finally {
            if(out != null) {
               if(var5 != null) {
                  try {
                     out.close();
                  } catch (Throwable var14) {
                     var5.addSuppressed(var14);
                  }
               } else {
                  out.close();
               }
            }

         }

      } catch (IOException var17) {
         throw new FSWriteError(var17, file.getPath());
      }
   }

   public long getFilePointer() {
      return this.dataFile.position();
   }

   public long getOnDiskFilePointer() {
      return this.dataFile.getOnDiskFilePointer();
   }

   public long getEstimatedOnDiskBytesWritten() {
      return this.dataFile.getEstimatedOnDiskBytesWritten();
   }

   static {
      WRITER_OPTION = SequentialWriterOption.newBuilder().trickleFsync(DatabaseDescriptor.getTrickleFsync()).trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKb() * 1024).bufferType(BufferType.OFF_HEAP).build();
   }

   class IndexWriter extends Transactional.AbstractTransactional implements Transactional {
      private final SequentialWriter rowIndexFile;
      public final FileHandle.Builder rowIndexFHBuilder;
      private final SequentialWriter partitionIndexFile;
      public final FileHandle.Builder partitionIndexFHBuilder;
      public final PartitionIndexBuilder partitionIndex;
      public final IFilter bf;
      boolean partitionIndexCompleted = false;
      private DataPosition riMark;
      private DataPosition piMark;

      IndexWriter(long keyCount) {
         this.rowIndexFile = new SequentialWriter(new File(TrieIndexSSTableWriter.this.descriptor.filenameFor(Component.ROW_INDEX)), TrieIndexSSTableWriter.WRITER_OPTION);
         this.rowIndexFHBuilder = SSTableReader.indexFileHandleBuilder(TrieIndexSSTableWriter.this.descriptor, TrieIndexSSTableWriter.this.metadata(), Component.ROW_INDEX);
         this.partitionIndexFile = new SequentialWriter(new File(TrieIndexSSTableWriter.this.descriptor.filenameFor(Component.PARTITION_INDEX)), TrieIndexSSTableWriter.WRITER_OPTION);
         this.partitionIndexFHBuilder = SSTableReader.indexFileHandleBuilder(TrieIndexSSTableWriter.this.descriptor, TrieIndexSSTableWriter.this.metadata(), Component.PARTITION_INDEX);
         this.partitionIndex = new PartitionIndexBuilder(this.partitionIndexFile, this.partitionIndexFHBuilder);
         this.bf = FilterFactory.getFilter(keyCount, TrieIndexSSTableWriter.this.metadata().params.bloomFilterFpChance, true);
         this.partitionIndexFile.setFileSyncListener(() -> {
            this.partitionIndex.markPartitionIndexSynced(this.partitionIndexFile.getLastFlushOffset());
         });
         this.rowIndexFile.setFileSyncListener(() -> {
            this.partitionIndex.markRowIndexSynced(this.rowIndexFile.getLastFlushOffset());
         });
         TrieIndexSSTableWriter.this.dataFile.setFileSyncListener(() -> {
            this.partitionIndex.markDataSynced(TrieIndexSSTableWriter.this.dataFile.getLastFlushOffset());
         });
      }

      public long append(DecoratedKey key, RowIndexEntry indexEntry) throws IOException {
         this.bf.add(key);
         long position;
         if(indexEntry.isIndexed()) {
            long indexStart = this.rowIndexFile.position();

            try {
               ByteBufferUtil.writeWithShortLength((ByteBuffer)key.getKey(), (DataOutputPlus)this.rowIndexFile);
               indexEntry.serialize(this.rowIndexFile, this.rowIndexFile.position());
            } catch (IOException var8) {
               throw new FSWriteError(var8, this.rowIndexFile.getPath());
            }

            if(TrieIndexSSTableWriter.logger.isTraceEnabled()) {
               TrieIndexSSTableWriter.logger.trace("wrote index entry: {} at {}", indexEntry, Long.valueOf(indexStart));
            }

            position = indexStart;
         } else {
            position = ~indexEntry.position;
         }

         this.partitionIndex.addEntry(key, position);
         return position;
      }

      public boolean buildPartial(long dataPosition, Consumer<PartitionIndex> callWhenReady) {
         return this.partitionIndex.buildPartial(callWhenReady, this.rowIndexFile.position(), dataPosition);
      }

      void flushBf() {
         if(TrieIndexSSTableWriter.this.components.contains(Component.FILTER)) {
            String path = TrieIndexSSTableWriter.this.descriptor.filenameFor(Component.FILTER);

            try {
               SeekableByteChannel fos = Files.newByteChannel(Paths.get(path, new String[0]), new OpenOption[]{StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE});
               Throwable var3 = null;

               try {
                  DataOutputStreamPlus stream = new BufferedDataOutputStreamPlus(fos);
                  Throwable var5 = null;

                  try {
                     FilterFactory.serialize(this.bf, stream);
                     stream.flush();
                     SyncUtil.sync((FileChannel)fos);
                  } catch (Throwable var30) {
                     var5 = var30;
                     throw var30;
                  } finally {
                     if(stream != null) {
                        if(var5 != null) {
                           try {
                              stream.close();
                           } catch (Throwable var29) {
                              var5.addSuppressed(var29);
                           }
                        } else {
                           stream.close();
                        }
                     }

                  }
               } catch (Throwable var32) {
                  var3 = var32;
                  throw var32;
               } finally {
                  if(fos != null) {
                     if(var3 != null) {
                        try {
                           fos.close();
                        } catch (Throwable var28) {
                           var3.addSuppressed(var28);
                        }
                     } else {
                        fos.close();
                     }
                  }

               }
            } catch (IOException var34) {
               throw new FSWriteError(var34, path);
            }
         }

      }

      public void mark() {
         this.riMark = this.rowIndexFile.mark();
         this.piMark = this.partitionIndexFile.mark();
      }

      public void resetAndTruncate() {
         this.rowIndexFile.resetAndTruncate(this.riMark);
         this.partitionIndexFile.resetAndTruncate(this.piMark);
      }

      protected void doPrepare() {
         this.flushBf();
         long position = this.rowIndexFile.position();
         this.rowIndexFile.prepareToCommit();
         FileUtils.truncate(this.rowIndexFile.getPath(), position);
         this.complete();
      }

      void complete() throws FSWriteError {
         if(!this.partitionIndexCompleted) {
            try {
               this.partitionIndex.complete();
               this.partitionIndexCompleted = true;
            } catch (IOException var2) {
               throw new FSWriteError(var2, this.partitionIndexFile.getPath());
            }
         }
      }

      PartitionIndex completedPartitionIndex() {
         this.complete();

         try {
            return PartitionIndex.load(this.partitionIndexFHBuilder, TrieIndexSSTableWriter.this.getPartitioner(), false);
         } catch (IOException var2) {
            throw new FSReadError(var2, this.partitionIndexFile.getPath());
         }
      }

      protected Throwable doCommit(Throwable accumulate) {
         return this.rowIndexFile.commit(accumulate);
      }

      protected Throwable doAbort(Throwable accumulate) {
         return this.rowIndexFile.abort(accumulate);
      }

      protected Throwable doPostCleanup(Throwable accumulate) {
         return Throwables.close(accumulate, ImmutableList.of(this.bf, this.partitionIndex, this.rowIndexFile, this.rowIndexFHBuilder, this.partitionIndexFile, this.partitionIndexFHBuilder));
      }
   }

   class TransactionalProxy extends SSTableWriter.TransactionalProxy {
      TransactionalProxy() {
         super();
      }

      protected void doPrepare() {
         TrieIndexSSTableWriter.this.iwriter.prepareToCommit();
         TrieIndexSSTableWriter.this.dataFile.prepareToCommit();
         TrieIndexSSTableWriter.this.writeMetadata(TrieIndexSSTableWriter.this.descriptor, TrieIndexSSTableWriter.this.finalizeMetadata());
         TrieIndexSSTableWriter.appendTOC(TrieIndexSSTableWriter.this.descriptor, TrieIndexSSTableWriter.this.components);
         if(this.openResult) {
            this.finalReader = TrieIndexSSTableWriter.this.openFinal(SSTableReader.OpenReason.NORMAL);
         }

      }

      protected Throwable doCommit(Throwable accumulate) {
         accumulate = TrieIndexSSTableWriter.this.dataFile.commit(accumulate);
         accumulate = TrieIndexSSTableWriter.this.iwriter.commit(accumulate);
         return accumulate;
      }

      protected Throwable doPostCleanup(Throwable accumulate) {
         accumulate = TrieIndexSSTableWriter.this.dbuilder.close(accumulate);
         return accumulate;
      }

      protected Throwable doAbort(Throwable accumulate) {
         accumulate = TrieIndexSSTableWriter.this.iwriter.abort(accumulate);
         accumulate = TrieIndexSSTableWriter.this.dataFile.abort(accumulate);
         return accumulate;
      }
   }

   private static class StatsCollector extends Transformation {
      private final MetadataCollector collector;

      StatsCollector(MetadataCollector collector) {
         this.collector = collector;
      }

      public Row applyToStatic(Row row) {
         if(!row.isEmpty()) {
            Rows.collectStats(row, this.collector);
         }

         return row;
      }

      public Row applyToRow(Row row) {
         this.collector.updateClusteringValues(row.clustering());
         Rows.collectStats(row, this.collector);
         return row;
      }

      public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker) {
         this.collector.updateClusteringValues(marker.clustering());
         if(marker.isBoundary()) {
            RangeTombstoneBoundaryMarker bm = (RangeTombstoneBoundaryMarker)marker;
            this.collector.update(bm.endDeletionTime());
            this.collector.update(bm.startDeletionTime());
         } else {
            this.collector.update(((RangeTombstoneBoundMarker)marker).deletionTime());
         }

         return marker;
      }

      public void onPartitionClose() {
         this.collector.addCellPerPartitionCount();
      }

      public DeletionTime applyToDeletion(DeletionTime deletionTime) {
         this.collector.update(deletionTime);
         return deletionTime;
      }
   }
}
