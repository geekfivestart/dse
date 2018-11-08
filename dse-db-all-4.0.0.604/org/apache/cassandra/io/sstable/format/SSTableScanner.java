package org.apache.cassandra.io.sstable.format;

import com.google.common.collect.Iterators;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.LazilyInitializedUnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.util.FileAccessType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Pair;

public class SSTableScanner implements ISSTableScanner {
   private final AtomicBoolean isClosed = new AtomicBoolean(false);
   protected final RandomAccessReader dfile;
   public final SSTableReader sstable;
   private final Iterator<AbstractBounds<PartitionPosition>> rangeIterator;
   private final ColumnFilter columns;
   private final DataRange dataRange;
   private final SSTableReadsListener listener;
   private long startScan = -1L;
   private long bytesScanned = 0L;
   protected CloseableIterator<UnfilteredRowIterator> iterator;

   public static ISSTableScanner getScanner(SSTableReader sstable) {
      return getScanner(sstable, (Iterator)Iterators.singletonIterator(fullRange(sstable)));
   }

   public static ISSTableScanner getScanner(SSTableReader sstable, ColumnFilter columns, DataRange dataRange, SSTableReadsListener listener) {
      return new SSTableScanner(sstable, columns, dataRange, makeBounds(sstable, dataRange).iterator(), listener);
   }

   public static ISSTableScanner getScanner(SSTableReader sstable, Collection<Range<Token>> tokenRanges) {
      List<Pair<Long, Long>> positions = sstable.getPositionsForRanges(tokenRanges);
      return (ISSTableScanner)(positions.isEmpty()?new SSTableScanner.EmptySSTableScanner(sstable):getScanner(sstable, makeBounds(sstable, tokenRanges).iterator()));
   }

   public static ISSTableScanner getScanner(SSTableReader sstable, Iterator<AbstractBounds<PartitionPosition>> rangeIterator) {
      return new SSTableScanner(sstable, ColumnFilter.all(sstable.metadata()), (DataRange)null, rangeIterator, SSTableReadsListener.NOOP_LISTENER);
   }

   private SSTableScanner(SSTableReader sstable, ColumnFilter columns, DataRange dataRange, Iterator<AbstractBounds<PartitionPosition>> rangeIterator, SSTableReadsListener listener) {
      assert sstable != null;

      this.dfile = sstable.openDataReader(FileAccessType.SEQUENTIAL);
      this.sstable = sstable;
      this.columns = columns;
      this.dataRange = dataRange;
      this.rangeIterator = rangeIterator;
      this.listener = listener;
   }

   public static List<AbstractBounds<PartitionPosition>> makeBounds(SSTableReader sstable, Collection<Range<Token>> tokenRanges) {
      List<AbstractBounds<PartitionPosition>> boundsList = new ArrayList(tokenRanges.size());
      Iterator var3 = Range.normalize(tokenRanges).iterator();

      while(var3.hasNext()) {
         Range<Token> range = (Range)var3.next();
         addRange(sstable, Range.makeRowRange(range), boundsList);
      }

      return boundsList;
   }

   static List<AbstractBounds<PartitionPosition>> makeBounds(SSTableReader sstable, DataRange dataRange) {
      List<AbstractBounds<PartitionPosition>> boundsList = new ArrayList(2);
      addRange(sstable, dataRange.keyRange(), boundsList);
      return boundsList;
   }

   static AbstractBounds<PartitionPosition> fullRange(SSTableReader sstable) {
      return new Bounds(sstable.first, sstable.last);
   }

   private static void addRange(SSTableReader sstable, AbstractBounds<PartitionPosition> requested, List<AbstractBounds<PartitionPosition>> boundsList) {
      AbstractBounds.Boundary right;
      AbstractBounds.Boundary left;
      if(requested instanceof Range && ((Range)requested).isWrapAround()) {
         if(((PartitionPosition)requested.right).compareTo(sstable.first) >= 0) {
            right = new AbstractBounds.Boundary(sstable.first, true);
            left = requested.rightBoundary();
            left = AbstractBounds.minRight(left, sstable.last, true);
            if(!AbstractBounds.isEmpty(right, left)) {
               boundsList.add(AbstractBounds.bounds(right, left));
            }
         }

         if(((PartitionPosition)requested.left).compareTo(sstable.last) <= 0) {
            right = new AbstractBounds.Boundary(sstable.last, true);
            left = requested.leftBoundary();
            left = AbstractBounds.maxLeft(left, sstable.first, true);
            if(!AbstractBounds.isEmpty(left, right)) {
               boundsList.add(AbstractBounds.bounds(left, right));
            }
         }
      } else {
         assert !AbstractBounds.strictlyWrapsAround(requested.left, requested.right);

         right = requested.leftBoundary();
         left = requested.rightBoundary();
         right = AbstractBounds.maxLeft(right, sstable.first, true);
         left = ((PartitionPosition)requested.right).isMinimum()?new AbstractBounds.Boundary(sstable.last, true):AbstractBounds.minRight(left, sstable.last, true);
         if(!AbstractBounds.isEmpty(right, left)) {
            boundsList.add(AbstractBounds.bounds(right, left));
         }
      }

   }

   public void close() {
      try {
         if(this.isClosed.compareAndSet(false, true)) {
            FileUtils.close(new Closeable[]{this.dfile});
            if(this.iterator != null) {
               this.iterator.close();
            }
         }

      } catch (IOException var2) {
         this.sstable.markSuspect();
         throw new CorruptSSTableException(var2, this.sstable.getFilename());
      }
   }

   public long getLengthInBytes() {
      return this.dfile.length();
   }

   public long getCurrentPosition() {
      return this.dfile.getFilePointer();
   }

   public long getBytesScanned() {
      return this.bytesScanned;
   }

   public long getCompressedLengthInBytes() {
      return this.sstable.onDiskLength();
   }

   public String getBackingFiles() {
      return this.sstable.toString();
   }

   public TableMetadata metadata() {
      return this.sstable.metadata();
   }

   public boolean hasNext() {
      if(this.iterator == null) {
         this.iterator = this.createIterator();
      }

      return this.iterator.hasNext();
   }

   public UnfilteredRowIterator next() {
      if(this.iterator == null) {
         this.iterator = this.createIterator();
      }

      return (UnfilteredRowIterator)this.iterator.next();
   }

   public void remove() {
      throw new UnsupportedOperationException();
   }

   private CloseableIterator<UnfilteredRowIterator> createIterator() {
      this.listener.onScanningStarted(this.sstable);
      return new SSTableScanner.KeyScanningIterator();
   }

   public String toString() {
      return this.getClass().getSimpleName() + "(dfile=" + this.dfile + " sstable=" + this.sstable + ")";
   }

   public static class EmptySSTableScanner extends AbstractUnfilteredPartitionIterator implements ISSTableScanner {
      private final SSTableReader sstable;

      public EmptySSTableScanner(SSTableReader sstable) {
         this.sstable = sstable;
      }

      public long getLengthInBytes() {
         return 0L;
      }

      public long getCurrentPosition() {
         return 0L;
      }

      public long getBytesScanned() {
         return 0L;
      }

      public long getCompressedLengthInBytes() {
         return 0L;
      }

      public String getBackingFiles() {
         return this.sstable.getFilename();
      }

      public TableMetadata metadata() {
         return this.sstable.metadata();
      }

      public void close() {
      }

      public boolean hasNext() {
         return false;
      }

      public UnfilteredRowIterator next() {
         return null;
      }
   }

   protected class KeyScanningIterator extends AbstractIterator<UnfilteredRowIterator> implements CloseableIterator<UnfilteredRowIterator> {
      private DecoratedKey currentKey;
      private RowIndexEntry currentEntry;
      private PartitionIndexIterator iterator;
      private LazilyInitializedUnfilteredRowIterator currentRowIterator;

      protected KeyScanningIterator() {
      }

      protected UnfilteredRowIterator computeNext() {
         if(this.currentRowIterator != null && this.currentRowIterator.initialized() && this.currentRowIterator.hasNext()) {
            throw new IllegalStateException("The UnfilteredRowIterator returned by the last call to next() was initialized: it should be either exhausted or closed before calling hasNext() or next() again.");
         } else {
            try {
               while(true) {
                  if(SSTableScanner.this.startScan != -1L) {
                     SSTableScanner.this.bytesScanned = SSTableScanner.this.bytesScanned + (SSTableScanner.this.dfile.getFilePointer() - SSTableScanner.this.startScan);
                  }

                  if(this.iterator != null) {
                     this.currentEntry = this.iterator.entry();
                     this.currentKey = this.iterator.key();
                     if(this.currentEntry != null) {
                        this.iterator.advance();
                        SSTableScanner.this.startScan = -1L;
                        this.currentRowIterator = new LazilyInitializedUnfilteredRowIterator(this.currentKey) {
                           protected UnfilteredRowIterator initializeIterator() {
                              try {
                                 if(SSTableScanner.this.startScan != -1L) {
                                    SSTableScanner.this.bytesScanned = SSTableScanner.this.bytesScanned + (SSTableScanner.this.dfile.getFilePointer() - SSTableScanner.this.startScan);
                                 }

                                 SSTableScanner.this.startScan = KeyScanningIterator.this.currentEntry.position;
                                 if(SSTableScanner.this.dataRange == null) {
                                    return SSTableScanner.this.sstable.simpleIterator(SSTableScanner.this.dfile, this.partitionKey(), KeyScanningIterator.this.currentEntry, false);
                                 } else {
                                    ClusteringIndexFilter filter = SSTableScanner.this.dataRange.clusteringIndexFilter(this.partitionKey());
                                    return SSTableScanner.this.sstable.iterator(SSTableScanner.this.dfile, this.partitionKey(), KeyScanningIterator.this.currentEntry, filter.getSlices(SSTableScanner.this.metadata()), SSTableScanner.this.columns, filter.isReversed());
                                 }
                              } catch (CorruptSSTableException var2) {
                                 SSTableScanner.this.sstable.markSuspect();
                                 throw new CorruptSSTableException(var2, SSTableScanner.this.sstable.getFilename());
                              }
                           }

                           public void close() {
                              super.close();
                              KeyScanningIterator.this.currentRowIterator = null;
                           }
                        };
                        return this.currentRowIterator;
                     }

                     this.iterator.close();
                     this.iterator = null;
                  }

                  if(!SSTableScanner.this.rangeIterator.hasNext()) {
                     return (UnfilteredRowIterator)this.endOfData();
                  }

                  this.iterator = SSTableScanner.this.sstable.coveredKeysIterator((AbstractBounds)SSTableScanner.this.rangeIterator.next());
               }
            } catch (IOException | CorruptSSTableException var2) {
               SSTableScanner.this.sstable.markSuspect();
               throw new CorruptSSTableException(var2, SSTableScanner.this.sstable.getFilename());
            }
         }
      }

      public void close() {
         if(this.iterator != null) {
            this.iterator.close();
         }

      }
   }
}
