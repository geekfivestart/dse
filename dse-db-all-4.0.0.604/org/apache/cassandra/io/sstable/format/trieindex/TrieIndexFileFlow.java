package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCScheduler;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.IndexFileEntry;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.flow.FlowSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrieIndexFileFlow extends FlowSource<IndexFileEntry> {
   private static final Logger logger = LoggerFactory.getLogger(TrieIndexFileFlow.class);
   private static final int MAX_RETRIES = 12;
   private final RandomAccessReader dataFileReader;
   private final TrieIndexSSTableReader sstable;
   private final PartitionIndex partitionIndex;
   private final IPartitioner partitioner;
   private final PartitionPosition left;
   private final int inclusiveLeft;
   private final PartitionPosition right;
   private final int exclusiveRight;
   private final FileHandle rowIndexFile;
   private final TPCScheduler onReadyExecutor;
   private FileDataInput rowIndexFileReader;
   private PartitionIndex.IndexPosIterator posIterator;
   private long pos = -9223372036854775808L;
   private IndexFileEntry next;

   public TrieIndexFileFlow(RandomAccessReader dataFileReader, TrieIndexSSTableReader sstable, PartitionPosition left, int inclusiveLeft, PartitionPosition right, int exclusiveRight) {
      this.dataFileReader = dataFileReader;
      this.sstable = sstable;
      this.partitionIndex = sstable.partitionIndex;
      this.partitioner = sstable.metadata().partitioner;
      this.left = left;
      this.inclusiveLeft = inclusiveLeft;
      this.right = right;
      this.exclusiveRight = exclusiveRight;
      this.rowIndexFile = sstable.rowIndexFile;
      this.onReadyExecutor = TPC.bestTPCScheduler();
   }

   public void requestNext() {
      this.readWithRetry(0);
   }

   private void readWithRetry(int retryNum) {
      try {
         if(retryNum >= 12) {
            throw new IOException(String.format("Too many NotInCacheExceptions (%d) in trie index flow", new Object[]{Integer.valueOf(retryNum)}));
         }

         IndexFileEntry current = this.next == null?this.readFirst():this.readNext();
         if(current != IndexFileEntry.EMPTY) {
            this.subscriber.onNext(current);
         } else {
            this.subscriber.onComplete();
         }
      } catch (Rebufferer.NotInCacheException var3) {
         if(logger.isTraceEnabled()) {
            logger.trace("{} - NotInCacheException at retry number {}: {}", new Object[]{Integer.valueOf(this.hashCode()), Integer.valueOf(retryNum), var3.getMessage()});
         }

         var3.accept(this.getClass(), () -> {
            this.readWithRetry(retryNum + 1);
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

   private void lateInitialization() {
      Rebufferer.ReaderConstraint rc = Rebufferer.ReaderConstraint.ASYNC;
      if(this.rowIndexFileReader == null) {
         this.rowIndexFileReader = this.rowIndexFile.createReader(rc);
      }

      if(this.posIterator == null) {
         this.posIterator = new PartitionIndex.IndexPosIterator(this.partitionIndex, this.left, this.right, rc);
      }

   }

   private IndexFileEntry readFirst() throws IOException {
      this.lateInitialization();
      this.next = this.readEntry();
      if(this.next.key != null && this.next.key.compareTo(this.left) <= this.inclusiveLeft) {
         this.next = null;
         this.next = this.readEntry();
      }

      return this.readNext();
   }

   private IndexFileEntry readNext() throws IOException {
      IndexFileEntry ret = this.next;
      if(ret == IndexFileEntry.EMPTY) {
         return ret;
      } else {
         this.next = this.readEntry();
         return this.next == IndexFileEntry.EMPTY && this.right != null && ret.key.compareTo(this.right) > this.exclusiveRight?IndexFileEntry.EMPTY:ret;
      }
   }

   private IndexFileEntry readEntry() throws IOException {
      if(this.pos == -9223372036854775808L) {
         this.pos = this.posIterator.nextIndexPos();
      }

      IndexFileEntry ret;
      if(this.pos != -9223372036854775808L) {
         if(this.pos >= 0L) {
            if(this.pos != this.rowIndexFileReader.getFilePointer()) {
               this.rowIndexFileReader.seek(this.pos);
            }

            ret = new IndexFileEntry(this.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(this.rowIndexFileReader)), TrieIndexEntry.deserialize(this.rowIndexFileReader, this.rowIndexFileReader.getFilePointer()));
         } else {
            long dataPos = ~this.pos;
            if(dataPos != this.dataFileReader.getFilePointer()) {
               this.dataFileReader.seek(dataPos);
            }

            ret = new IndexFileEntry(this.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(this.dataFileReader)), new RowIndexEntry(dataPos));
         }

         this.pos = -9223372036854775808L;
      } else {
         ret = IndexFileEntry.EMPTY;
      }

      return ret;
   }

   public void close() throws Exception {
      Throwable err = Throwables.closeNonNull((Throwable)null, (AutoCloseable[])(new AutoCloseable[]{this.posIterator, this.rowIndexFileReader}));
      Throwables.maybeFail(err);
   }
}
