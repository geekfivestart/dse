package org.apache.cassandra.io.sstable.format;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCScheduler;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.PartitionHeader;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileAccessType;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.FlowSource;
import org.apache.cassandra.utils.flow.FlowSubscriber;
import org.apache.cassandra.utils.flow.FlowSubscriptionRecipient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AsyncPartitionReader {
   private static final Logger logger = LoggerFactory.getLogger(AsyncPartitionReader.class);
   final SSTableReadsListener listener;
   final DecoratedKey key;
   final ColumnFilter selectedColumns;
   final SSTableReader table;
   final boolean reverse;
   final SerializationHelper helper;
   final boolean lowerBoundAllowed;
   Slices slices;
   final AtomicReference<AsyncPartitionReader.DataFileOwner> dataFileOwner;
   volatile RowIndexEntry indexEntry = null;
   volatile FileDataInput dfile = null;
   volatile long filePos = -1L;
   volatile AsyncPartitionReader.State state;
   Row staticRow;
   DeletionTime partitionLevelDeletion;

   private AsyncPartitionReader(SSTableReader table, SSTableReadsListener listener, DecoratedKey key, RowIndexEntry indexEntry, FileDataInput dfile, Slices slices, ColumnFilter selectedColumns, boolean reverse, boolean lowerBoundAllowed) {
      this.staticRow = Rows.EMPTY_STATIC_ROW;
      this.partitionLevelDeletion = DeletionTime.LIVE;
      this.table = table;
      this.listener = listener;
      this.indexEntry = indexEntry;
      this.dfile = dfile;
      this.key = key;
      this.selectedColumns = selectedColumns;
      this.slices = slices;
      this.reverse = reverse;
      this.helper = new SerializationHelper(table.metadata(), table.descriptor.version.encodingVersion(), SerializationHelper.Flag.LOCAL, selectedColumns);
      this.lowerBoundAllowed = lowerBoundAllowed;
      this.dataFileOwner = new AtomicReference(dfile != null?AsyncPartitionReader.DataFileOwner.EXTERNAL:AsyncPartitionReader.DataFileOwner.NONE);
      this.state = indexEntry == null?AsyncPartitionReader.State.STARTING:AsyncPartitionReader.State.HAVE_INDEX_ENTRY;
   }

   public static Flow<FlowableUnfilteredPartition> create(SSTableReader table, SSTableReadsListener listener, DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reverse, boolean lowerBoundAllowed) {
      return (new AsyncPartitionReader(table, listener, key, (RowIndexEntry)null, (FileDataInput)null, slices, selectedColumns, reverse, lowerBoundAllowed)).partitions();
   }

   public static Flow<FlowableUnfilteredPartition> create(SSTableReader table, FileDataInput dfile, SSTableReadsListener listener, IndexFileEntry indexEntry) {
      return (new AsyncPartitionReader(table, listener, indexEntry.key, indexEntry.entry, dfile, Slices.ALL, ColumnFilter.all(table.metadata()), false, false)).partitions();
   }

   public static Flow<FlowableUnfilteredPartition> create(SSTableReader table, FileDataInput dfile, SSTableReadsListener listener, IndexFileEntry indexEntry, Slices slices, ColumnFilter selectedColumns, boolean reversed) {
      return (new AsyncPartitionReader(table, listener, indexEntry.key, indexEntry.entry, dfile, slices, selectedColumns, reversed, false)).partitions();
   }

   boolean canUseLowerBound() {
      return this.lowerBoundAllowed && !this.table.mayHaveTombstones() && !this.table.metadata().isCompactTable() && !this.table.header.hasStatic() && this.selectedColumns.fetchedColumns().statics.isEmpty();
   }

   private ClusteringBound getMetadataLowerBound() {
      StatsMetadata m = this.table.getSSTableMetadata();
      List<ByteBuffer> vals = this.reverse?m.maxClusteringValues:m.minClusteringValues;
      return ClusteringBound.inclusiveOpen(this.reverse, (ByteBuffer[])vals.toArray(new ByteBuffer[vals.size()]));
   }

   public Flow<FlowableUnfilteredPartition> partitions() {
      if(this.canUseLowerBound()) {
         PartitionHeader header = new PartitionHeader(this.table.metadata(), this.key, DeletionTime.LIVE, this.selectedColumns.fetchedColumns(), this.reverse, this.table.stats);
         return Flow.just(new AsyncPartitionReader.PartitionSubscription(header, Rows.EMPTY_STATIC_ROW, true));
      } else {
         return new AsyncPartitionReader.PartitionReader();
      }
   }

   private void readWithRetry(AsyncPartitionReader.Reader reader, Rebufferer.NotInCacheException retryException, TPCScheduler onReadyExecutor) {
      boolean isRetry = retryException != null;

      try {
         reader.performRead(isRetry);
      } catch (Rebufferer.NotInCacheException var6) {
         var6.accept(this.getClass(), () -> {
            this.readWithRetry(reader, var6, onReadyExecutor);
         }, (t) -> {
            if(t instanceof CompletionException && t.getCause() != null) {
               t = t.getCause();
            }

            reader.onError(t);
            return null;
         }, onReadyExecutor);
      } catch (IndexOutOfBoundsException | IOException var7) {
         if(Rebufferer.NotInCacheException.DEBUG && retryException != null) {
            logger.error("Failed to read on retry, reader was: {}, last NotInCacheException was: ", reader, retryException);
         }

         reader.table().markSuspect();
         reader.onError(new CorruptSSTableException(var7, reader.table().getFilename()));
      } catch (Throwable var8) {
         reader.onError(var8);
      }

   }

   public boolean prepareRow() throws Exception {
      if(this.filePos != -1L) {
         this.dfile.seek(this.filePos);
      }

      label52: {
         switch(null.$SwitchMap$org$apache$cassandra$io$sstable$format$AsyncPartitionReader$State[this.state.ordinal()]) {
         case 1:
            assert this.indexEntry == null;

            this.indexEntry = this.table.getExactPosition(this.key, this.listener, Rebufferer.ReaderConstraint.ASYNC);
            if(this.indexEntry == null) {
               this.state = AsyncPartitionReader.State.PREPARED;
               return false;
            }

            this.state = AsyncPartitionReader.State.HAVE_INDEX_ENTRY;
         case 2:
            boolean needSeekAtPartitionStart = !this.indexEntry.isIndexed() || !this.selectedColumns.fetchedColumns().statics.isEmpty();
            if(!needSeekAtPartitionStart) {
               this.partitionLevelDeletion = this.indexEntry.deletionTime();
               this.staticRow = Rows.EMPTY_STATIC_ROW;
               this.state = AsyncPartitionReader.State.PREPARED;
               return true;
            }

            if(this.dataFileOwner.compareAndSet(AsyncPartitionReader.DataFileOwner.NONE, AsyncPartitionReader.DataFileOwner.PARTITION_READER)) {
               this.dfile = this.table.openDataReader(Rebufferer.ReaderConstraint.ASYNC, FileAccessType.RANDOM);
            }

            this.filePos = this.indexEntry.position;
            this.state = AsyncPartitionReader.State.HAVE_DFILE;
            this.dfile.seek(this.filePos);
         case 3:
            ByteBufferUtil.skipShortLength(this.dfile);
            this.filePos = this.dfile.getFilePointer();
            this.state = AsyncPartitionReader.State.HAVE_SKIPPED_KEY;
         case 4:
            break;
         case 5:
            break label52;
         case 6:
         default:
            throw new AssertionError();
         }

         this.partitionLevelDeletion = DeletionTime.serializer.deserialize(this.dfile);
         this.filePos = this.dfile.getFilePointer();
         this.state = AsyncPartitionReader.State.HAVE_DELETION_TIME;
      }

      this.staticRow = SSTableReader.readStaticRow(this.table, this.dfile, this.helper, this.selectedColumns.fetchedColumns().statics);
      this.filePos = this.dfile.getFilePointer();
      this.state = AsyncPartitionReader.State.PREPARED;
      return true;
   }

   class PartitionSubscription extends FlowableUnfilteredPartition.FlowSource implements AsyncPartitionReader.Reader {
      final TPCScheduler onReadyExecutor = TPC.bestTPCScheduler();
      SSTableReader.PartitionReader sstableReader;
      boolean provideLowerBound;

      protected PartitionSubscription(PartitionHeader header, Row staticRow, boolean provideLowerBound) {
         super(header, staticRow);
         this.provideLowerBound = provideLowerBound;
      }

      private FileDataInput openFile() {
         if(AsyncPartitionReader.this.dataFileOwner.compareAndSet(AsyncPartitionReader.DataFileOwner.PARTITION_READER, AsyncPartitionReader.DataFileOwner.PARTITION_SUBSCRIPTION)) {
            return AsyncPartitionReader.this.dfile;
         } else {
            switch(null.$SwitchMap$org$apache$cassandra$io$sstable$format$AsyncPartitionReader$DataFileOwner[((AsyncPartitionReader.DataFileOwner)AsyncPartitionReader.this.dataFileOwner.get()).ordinal()]) {
            case 1:
               return AsyncPartitionReader.this.dfile;
            case 2:
               return AsyncPartitionReader.this.dfile;
            case 3:
               AsyncPartitionReader.this.dataFileOwner.set(AsyncPartitionReader.DataFileOwner.PARTITION_SUBSCRIPTION);
               return AsyncPartitionReader.this.dfile = AsyncPartitionReader.this.table.openDataReader(Rebufferer.ReaderConstraint.ASYNC, FileAccessType.RANDOM);
            default:
               throw new AssertionError();
            }
         }
      }

      public void performRead(boolean isRetry) throws Exception {
         if(this.sstableReader == null) {
            if(AsyncPartitionReader.this.state != AsyncPartitionReader.State.PREPARED && !AsyncPartitionReader.this.prepareRow()) {
               this.subscriber.onComplete();
               return;
            }

            this.openFile();
            if(AsyncPartitionReader.this.filePos != -1L) {
               AsyncPartitionReader.this.dfile.seek(AsyncPartitionReader.this.filePos);
            }

            this.sstableReader = AsyncPartitionReader.this.table.reader(AsyncPartitionReader.this.dfile, false, AsyncPartitionReader.this.indexEntry, AsyncPartitionReader.this.helper, AsyncPartitionReader.this.slices, AsyncPartitionReader.this.reverse, Rebufferer.ReaderConstraint.ASYNC);
         } else if(isRetry) {
            this.sstableReader.resetReaderState();
         }

         Unfiltered next = this.sstableReader.next();
         if(next != null) {
            this.subscriber.onNext(next);
         } else {
            this.subscriber.onComplete();
         }

      }

      public void onError(Throwable t) {
         this.subscriber.onError(t);
      }

      public SSTableReader table() {
         return AsyncPartitionReader.this.table;
      }

      public void requestNext() {
         AsyncPartitionReader.this.readWithRetry(this, (Rebufferer.NotInCacheException)null, this.onReadyExecutor);
      }

      public void close() throws Exception {
         try {
            if(this.sstableReader != null) {
               this.sstableReader.close();
            }
         } finally {
            if(AsyncPartitionReader.this.dataFileOwner.compareAndSet(AsyncPartitionReader.DataFileOwner.PARTITION_SUBSCRIPTION, AsyncPartitionReader.DataFileOwner.NONE)) {
               AsyncPartitionReader.this.dfile.close();
            }

         }

      }

      public String toString() {
         return Flow.formatTrace(String.format("PartitionSubscription: %s, sstable reader: [%s]", new Object[]{AsyncPartitionReader.this.table, this.sstableReader}));
      }

      public FlowableUnfilteredPartition skipLowerBound() {
         assert this.subscriber == null;

         this.provideLowerBound = false;
         return this;
      }

      public void requestFirst(FlowSubscriber<Unfiltered> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
         this.subscribe(subscriber, subscriptionRecipient);
         if(this.provideLowerBound) {
            subscriber.onNext(new RangeTombstoneBoundMarker(AsyncPartitionReader.this.getMetadataLowerBound(), DeletionTime.LIVE));
         } else {
            this.requestNext();
         }

      }
   }

   class PartitionReader extends FlowSource<FlowableUnfilteredPartition> implements AsyncPartitionReader.Reader {
      final TPCScheduler onReadyExecutor = TPC.bestTPCScheduler();

      PartitionReader() {
      }

      public void performRead(boolean isRetry) throws Exception {
         if(!AsyncPartitionReader.this.prepareRow()) {
            this.subscriber.onComplete();
         } else {
            PartitionHeader header = new PartitionHeader(AsyncPartitionReader.this.table.metadata(), AsyncPartitionReader.this.key, AsyncPartitionReader.this.partitionLevelDeletion, AsyncPartitionReader.this.selectedColumns.fetchedColumns(), AsyncPartitionReader.this.reverse, AsyncPartitionReader.this.table.stats());
            AsyncPartitionReader.PartitionSubscription partitionContent = AsyncPartitionReader.this.new PartitionSubscription(header, AsyncPartitionReader.this.staticRow, false);
            this.subscriber.onFinal(partitionContent);
         }
      }

      public void onError(Throwable t) {
         this.subscriber.onError(t);
      }

      public SSTableReader table() {
         return AsyncPartitionReader.this.table;
      }

      public void requestNext() {
         AsyncPartitionReader.this.readWithRetry(this, (Rebufferer.NotInCacheException)null, this.onReadyExecutor);
      }

      public void close() throws Exception {
         if(AsyncPartitionReader.this.dataFileOwner.compareAndSet(AsyncPartitionReader.DataFileOwner.PARTITION_READER, AsyncPartitionReader.DataFileOwner.NONE)) {
            AsyncPartitionReader.this.dfile.close();
         }

      }

      public String toString() {
         return Flow.formatTrace("PartitionReader:" + AsyncPartitionReader.this.table);
      }
   }

   static enum State {
      STARTING,
      HAVE_INDEX_ENTRY,
      HAVE_DFILE,
      HAVE_SKIPPED_KEY,
      HAVE_DELETION_TIME,
      PREPARED;

      private State() {
      }
   }

   interface Reader {
      void performRead(boolean var1) throws Exception;

      void onError(Throwable var1);

      SSTableReader table();
   }

   static enum DataFileOwner {
      EXTERNAL,
      PARTITION_READER,
      PARTITION_SUBSCRIPTION,
      NONE;

      private DataFileOwner() {
      }
   }
}
