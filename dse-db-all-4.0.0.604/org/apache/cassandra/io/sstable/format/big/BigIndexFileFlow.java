package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCScheduler;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.IndexFileEntry;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.flow.FlowSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigIndexFileFlow extends FlowSource<IndexFileEntry> {
   private static final Logger logger = LoggerFactory.getLogger(BigIndexFileFlow.class);
   private final BigTableReader sstable;
   private final FileHandle ifile;
   private final RandomAccessReader reader;
   private final BigRowIndexEntry.IndexSerializer rowIndexEntrySerializer;
   private final IPartitioner partitioner;
   private final Version version;
   private final PartitionPosition left;
   private final int inclusiveLeft;
   private final PartitionPosition right;
   private final int exclusiveRight;
   private final TPCScheduler onReadyExecutor;
   private boolean firstPublished;
   private long position;

   public BigIndexFileFlow(BigTableReader sstable, PartitionPosition left, int inclusiveLeft, PartitionPosition right, int exclusiveRight) {
      this.sstable = sstable;
      this.ifile = sstable.ifile.sharedCopy();
      this.reader = this.ifile.createReader(Rebufferer.ReaderConstraint.ASYNC);
      this.rowIndexEntrySerializer = sstable.rowIndexEntrySerializer;
      this.partitioner = sstable.getPartitioner();
      this.version = sstable.descriptor.version;
      this.left = left;
      this.inclusiveLeft = inclusiveLeft;
      this.right = right;
      this.exclusiveRight = exclusiveRight;
      this.onReadyExecutor = TPC.bestTPCScheduler();
      this.firstPublished = false;
      this.position = -1L;
   }

   public void requestNext() {
      this.readWithRetry(false);
   }

   private void readWithRetry(boolean isRetry) {
      try {
         IndexFileEntry current = !this.firstPublished?this.readFirst(isRetry):this.readNext(isRetry);
         if(current != IndexFileEntry.EMPTY) {
            this.subscriber.onNext(current);
         } else {
            this.subscriber.onComplete();
         }
      } catch (Rebufferer.NotInCacheException var3) {
         if(logger.isTraceEnabled()) {
            logger.trace("{} - isRetry? {}, firstPublished? {}  NotInCacheException: {}", new Object[]{Integer.valueOf(this.hashCode()), Boolean.valueOf(isRetry), Boolean.valueOf(this.firstPublished), var3.getMessage()});
         }

         var3.accept(this.getClass(), () -> {
            this.readWithRetry(true);
         }, (t) -> {
            logger.error("Failed to retry due to exception", t);
            if(t instanceof CompletionException && t.getCause() != null) {
               t = t.getCause();
            }

            this.subscriber.onError(t);
            return null;
         }, this.onReadyExecutor);
      } catch (IOException | CorruptSSTableException var4) {
         this.sstable.markSuspect();
         this.subscriber.onError(new CorruptSSTableException(var4, this.sstable.getFilename()));
      } catch (Throwable var5) {
         this.subscriber.onError(var5);
      }

   }

   private IndexFileEntry readFirst(boolean isRetry) throws IOException {
      if(!isRetry) {
         assert this.position == -1L : "readFirst called multiple times with retry set to false";

         this.position = this.sstable.getIndexScanPosition(this.left);
      }

      this.reader.seek(this.position);

      while(!this.reader.isEOF()) {
         DecoratedKey indexDecoratedKey = this.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(this.reader));
         if(indexDecoratedKey.compareTo(this.left) > this.inclusiveLeft) {
            if(indexDecoratedKey.compareTo(this.right) <= this.exclusiveRight) {
               this.firstPublished = true;
               return new IndexFileEntry(indexDecoratedKey, this.rowIndexEntrySerializer.deserialize(this.reader, this.reader.getFilePointer()));
            }
            break;
         }

         BigRowIndexEntry.Serializer.skip(this.reader, this.version);
         this.position = this.reader.getPosition();
      }

      return IndexFileEntry.EMPTY;
   }

   private IndexFileEntry readNext(boolean isRetry) throws IOException {
      if(isRetry) {
         this.reader.seek(this.position);
      } else {
         this.position = this.reader.getPosition();
      }

      if(!this.reader.isEOF()) {
         DecoratedKey indexDecoratedKey = this.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(this.reader));
         if(this.right == null || indexDecoratedKey.compareTo(this.right) <= this.exclusiveRight) {
            return new IndexFileEntry(indexDecoratedKey, this.rowIndexEntrySerializer.deserialize(this.reader, this.reader.getFilePointer()));
         }
      }

      return IndexFileEntry.EMPTY;
   }

   public void close() throws Exception {
      Throwables.maybeFail(Throwables.closeNonNull((Throwable)null, (AutoCloseable[])(new AutoCloseable[]{this.reader, this.ifile})));
   }
}
