package org.apache.cassandra.db.compaction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import java.io.Closeable;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.cassandra.db.Clusterable;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.ArrayBackedPartition;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.ScrubPartitionIterator;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileAccessType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.cassandra.utils.memory.HeapAllocator;

public class Scrubber implements Closeable {
   private final ColumnFamilyStore cfs;
   private final SSTableReader sstable;
   private final LifecycleTransaction transaction;
   private final File destination;
   private final boolean skipCorrupted;
   private final boolean isCommutative;
   private final boolean isIndex;
   private final boolean checkData;
   private final boolean reinsertOverflowedTTLRows;
   private final long expectedBloomFilterSize;
   private final RandomAccessReader dataFile;
   private final Scrubber.ScrubInfo scrubInfo;
   private final ScrubPartitionIterator indexIterator;
   private int goodRows;
   private int badRows;
   private int emptyRows;
   private ByteBuffer currentIndexKey;
   private Scrubber.NegativeLocalDeletionInfoMetrics negativeLocalDeletionInfoMetrics;
   private final OutputHandler outputHandler;
   private static final Comparator<Partition> partitionComparator = new Comparator<Partition>() {
      public int compare(Partition r1, Partition r2) {
         return r1.partitionKey().compareTo((PartitionPosition)r2.partitionKey());
      }
   };
   private final SortedSet<Partition> outOfOrder;

   public Scrubber(ColumnFamilyStore cfs, LifecycleTransaction transaction, boolean skipCorrupted, boolean checkData) {
      this(cfs, transaction, skipCorrupted, checkData, false);
   }

   public Scrubber(ColumnFamilyStore cfs, LifecycleTransaction transaction, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTLRows) {
      this(cfs, transaction, skipCorrupted, new OutputHandler.LogOutput(), checkData, reinsertOverflowedTTLRows);
   }

   public Scrubber(ColumnFamilyStore cfs, LifecycleTransaction transaction, boolean skipCorrupted, OutputHandler outputHandler, boolean checkData, boolean reinsertOverflowedTTLRows) {
      this.negativeLocalDeletionInfoMetrics = new Scrubber.NegativeLocalDeletionInfoMetrics();
      this.outOfOrder = new TreeSet(partitionComparator);
      this.cfs = cfs;
      this.transaction = transaction;
      this.sstable = transaction.onlyOne();
      this.outputHandler = outputHandler;
      this.skipCorrupted = skipCorrupted;
      this.reinsertOverflowedTTLRows = reinsertOverflowedTTLRows;
      List<SSTableReader> toScrub = Collections.singletonList(this.sstable);
      long writeSize = cfs.getExpectedCompactedFileSize(toScrub, OperationType.SCRUB);
      this.destination = cfs.getDirectories().getWriteableLocationAsFile(cfs, this.sstable, writeSize);
      this.isCommutative = cfs.metadata().isCounter();
      this.isIndex = cfs.isIndex();
      this.indexIterator = this.openIndexIterator();
      boolean hasIndexFile = this.indexIterator != null;
      if(!hasIndexFile) {
         outputHandler.warn("Missing component: " + this.sstable.descriptor.filenameFor(Component.PARTITION_INDEX));
      }

      this.checkData = checkData && !this.isIndex;
      this.expectedBloomFilterSize = Math.max((long)cfs.metadata().params.minIndexInterval, hasIndexFile?SSTableReader.getApproximateKeyCount(toScrub):0L);
      this.dataFile = transaction.isOffline()?this.sstable.openDataReader(FileAccessType.FULL_FILE):this.sstable.openDataReader(CompactionManager.instance.getRateLimiter(), FileAccessType.FULL_FILE);
      this.scrubInfo = new Scrubber.ScrubInfo(this.dataFile, this.sstable);
      if(reinsertOverflowedTTLRows) {
         outputHandler.output("Starting scrub with reinsert overflowed TTL option");
      }

   }

   private ScrubPartitionIterator openIndexIterator() {
      try {
         return this.sstable.scrubPartitionsIterator();
      } catch (IOException var2) {
         this.outputHandler.warn("Index is unreadable.");
         return null;
      }
   }

   private UnfilteredRowIterator withValidation(UnfilteredRowIterator iter, String filename) {
      this.validatePrimaryKey(iter.partitionKey().getKey(), filename);
      return this.checkData?UnfilteredRowIterators.withValidation(iter, filename):iter;
   }

   public void validatePrimaryKey(ByteBuffer pk, String filename) {
      try {
         this.cfs.metadata().partitionKeyType.validate(pk);
      } catch (Exception var4) {
         throw new CorruptSSTableException(var4, filename);
      }
   }

   public void scrub() {
      List<SSTableReader> finished = new ArrayList();
      boolean completed = false;
      this.outputHandler.output(String.format("Scrubbing %s (%s)", new Object[]{this.sstable, FBUtilities.prettyPrintMemory(this.dataFile.length())}));

      try {
         SSTableRewriter writer = SSTableRewriter.construct(this.cfs, this.transaction, false, this.sstable.maxDataAge);
         Throwable var4 = null;

         try {
            Refs<SSTableReader> refs = Refs.ref(Collections.singleton(this.sstable));
            Throwable var6 = null;

            try {
               if(this.indexIterator != null) {
                  long firstRowPositionFromIndex = this.indexIterator.dataPosition();

                  assert firstRowPositionFromIndex == 0L : firstRowPositionFromIndex;
               }

               StatsMetadata metadata = this.sstable.getSSTableMetadata();
               writer.switchWriter(CompactionManager.createWriter(this.cfs, this.destination, this.expectedBloomFilterSize, metadata.repairedAt, metadata.pendingRepair, this.sstable, this.transaction));
               DecoratedKey prevKey = null;

               while(true) {
                  long rowStart;
                  if(this.dataFile.isEOF()) {
                     if(!this.outOfOrder.isEmpty()) {
                        rowStart = this.badRows > 0?0L:metadata.repairedAt;
                        SSTableWriter inOrderWriter = CompactionManager.createWriter(this.cfs, this.destination, this.expectedBloomFilterSize, rowStart, metadata.pendingRepair, this.sstable, this.transaction);
                        Throwable var13 = null;

                        SSTableReader newInOrderSstable;
                        try {
                           Iterator var112 = this.outOfOrder.iterator();

                           while(var112.hasNext()) {
                              Partition partition = (Partition)var112.next();
                              inOrderWriter.append(partition.unfilteredIterator());
                           }

                           newInOrderSstable = inOrderWriter.finish(-1L, this.sstable.maxDataAge, true);
                        } catch (Throwable var101) {
                           var13 = var101;
                           throw var101;
                        } finally {
                           if(inOrderWriter != null) {
                              if(var13 != null) {
                                 try {
                                    inOrderWriter.close();
                                 } catch (Throwable var97) {
                                    var13.addSuppressed(var97);
                                 }
                              } else {
                                 inOrderWriter.close();
                              }
                           }

                        }

                        this.transaction.update(newInOrderSstable, false);
                        finished.add(newInOrderSstable);
                        this.outputHandler.warn(String.format("%d out of order rows found while scrubbing %s; Those have been written (in order) to a new sstable (%s)", new Object[]{Integer.valueOf(this.outOfOrder.size()), this.sstable, newInOrderSstable}));
                     }

                     finished.addAll(writer.setRepairedAt(this.badRows > 0?0L:this.sstable.getSSTableMetadata().repairedAt).finish());
                     completed = true;
                     break;
                  }

                  if(this.scrubInfo.isStopRequested()) {
                     throw new CompactionInterruptedException(this.scrubInfo.getCompactionInfo());
                  }

                  rowStart = this.dataFile.getFilePointer();
                  this.outputHandler.debug("Reading row at " + rowStart);
                  DecoratedKey key = null;

                  try {
                     key = this.sstable.decorateKey(ByteBufferUtil.readWithShortLength(this.dataFile));
                  } catch (Throwable var99) {
                     this.throwIfFatal(var99);
                  }

                  long dataStart = this.dataFile.getFilePointer();
                  long rowSizeFromIndex = -1L;
                  long rowStartFromIndex = -1L;
                  this.currentIndexKey = null;
                  if(this.indexIterator != null) {
                     this.currentIndexKey = this.indexIterator.key();
                     rowStartFromIndex = this.indexIterator.dataPosition();
                     if(this.indexIterator.dataPosition() != -1L) {
                        this.indexIterator.advance();
                        if(this.indexIterator.dataPosition() != -1L) {
                           rowSizeFromIndex = this.indexIterator.dataPosition() - rowStartFromIndex;
                        }
                     }
                  }

                  String keyName = key == null?"(unreadable key)":ByteBufferUtil.bytesToHex(key.getKey());
                  this.outputHandler.debug(String.format("row %s is %s", new Object[]{keyName, FBUtilities.prettyPrintMemory(rowSizeFromIndex)}));

                  try {
                     if(key == null) {
                        throw new IOError(new IOException("Unable to read row key from data file"));
                     }

                     if(this.currentIndexKey != null && !key.getKey().equals(this.currentIndexKey)) {
                        throw new IOError(new IOException(String.format("Key from data file (%s) does not match key from index file (%s)", new Object[]{"_too big_", ByteBufferUtil.bytesToHex(this.currentIndexKey)})));
                     }

                     if(this.indexIterator != null && rowSizeFromIndex > this.dataFile.length()) {
                        throw new IOError(new IOException("Impossible row size (greater than file length): " + rowSizeFromIndex));
                     }

                     if(this.indexIterator != null && rowStart != rowStartFromIndex) {
                        this.outputHandler.warn(String.format("Data file row position %d differs from index file row position %d", new Object[]{Long.valueOf(rowStart), Long.valueOf(rowStartFromIndex)}));
                     }

                     if(this.indexIterator != null && rowSizeFromIndex > 0L && dataStart - rowStart >= rowSizeFromIndex) {
                        throw new IOException(String.format("Reading key with length %d at position %d already makes row longer than size from index %d. Key read is %s\nThis most probably indicates a corrupted row, thus scrub will continue assuming the index file contains the correct data.", new Object[]{Integer.valueOf(key.getKey().remaining()), Long.valueOf(rowStart), Long.valueOf(rowSizeFromIndex), ByteBufferUtil.bytesToHex(key.getKey())}));
                     }

                     if(this.tryAppend(prevKey, key, writer)) {
                        prevKey = key;
                     }
                  } catch (Throwable var100) {
                     this.throwIfFatal(var100);
                     this.outputHandler.warn("Error reading row (stacktrace follows):", var100);
                     if(this.currentIndexKey != null && (key == null || !key.getKey().equals(this.currentIndexKey) || rowStart != rowStartFromIndex)) {
                        long dataStartFromIndex = rowStartFromIndex + 2L + (long)this.currentIndexKey.remaining();
                        this.outputHandler.output(String.format("Retrying from row index; data is %s bytes starting at %s", new Object[]{Long.valueOf(rowSizeFromIndex), Long.valueOf(rowStartFromIndex)}));
                        key = this.sstable.decorateKey(this.currentIndexKey);

                        try {
                           this.dataFile.seek(dataStartFromIndex);
                           if(this.tryAppend(prevKey, key, writer)) {
                              prevKey = key;
                           }
                        } catch (Throwable var98) {
                           this.throwIfFatal(var98);
                           this.throwIfCannotContinue(key, var98);
                           this.outputHandler.warn("Retry failed too. Skipping to next row (retry's stacktrace follows)", var98);
                           ++this.badRows;
                           this.seekToNextRow();
                        }
                     } else {
                        this.throwIfCannotContinue(key, var100);
                        ++this.badRows;
                        if(this.indexIterator == null) {
                           throw var100;
                        }

                        this.outputHandler.warn("Row starting at position " + rowStart + " is unreadable; skipping to next");
                        this.seekToNextRow();
                     }
                  }
               }
            } catch (Throwable var103) {
               var6 = var103;
               throw var103;
            } finally {
               if(refs != null) {
                  if(var6 != null) {
                     try {
                        refs.close();
                     } catch (Throwable var96) {
                        var6.addSuppressed(var96);
                     }
                  } else {
                     refs.close();
                  }
               }

            }
         } catch (Throwable var105) {
            var4 = var105;
            throw var105;
         } finally {
            if(writer != null) {
               if(var4 != null) {
                  try {
                     writer.close();
                  } catch (Throwable var95) {
                     var4.addSuppressed(var95);
                  }
               } else {
                  writer.close();
               }
            }

         }
      } catch (IOException var107) {
         throw Throwables.propagate(var107);
      } finally {
         if(this.transaction.isOffline()) {
            finished.forEach((sstable) -> {
               sstable.selfRef().release();
            });
         }

      }

      if(completed) {
         this.outputHandler.output("Scrub of " + this.sstable + " complete: " + this.goodRows + " rows in new sstable and " + this.emptyRows + " empty (tombstoned) rows dropped");
         if(this.negativeLocalDeletionInfoMetrics.fixedRows > 0) {
            this.outputHandler.output("Fixed " + this.negativeLocalDeletionInfoMetrics.fixedRows + " rows with overflowed local deletion time.");
         }

         if(this.badRows > 0) {
            this.outputHandler.warn("Unable to recover " + this.badRows + " rows that were skipped.  You can attempt manual recovery from the pre-scrub snapshot.  You can also run nodetool repair to transfer the data from a healthy replica, if any");
         }
      } else if(this.badRows > 0) {
         this.outputHandler.warn("No valid rows found while scrubbing " + this.sstable + "; it is marked for deletion now. If you want to attempt manual recovery, you can find a copy in the pre-scrub snapshot");
      } else {
         this.outputHandler.output("Scrub of " + this.sstable + " complete; looks like all " + this.emptyRows + " rows were tombstoned");
      }

   }

   private boolean tryAppend(DecoratedKey prevKey, DecoratedKey key, SSTableRewriter writer) {
      Scrubber.OrderCheckerIterator sstableIterator = new Scrubber.OrderCheckerIterator(this.getIterator(key), this.cfs.metadata().comparator);
      UnfilteredRowIterator iterator = this.withValidation(sstableIterator, this.dataFile.getPath());
      Throwable var6 = null;

      label115: {
         boolean var7;
         try {
            if(prevKey == null || prevKey.compareTo((PartitionPosition)key) <= 0) {
               if(writer.tryAppend(iterator) == null) {
                  ++this.emptyRows;
               } else {
                  ++this.goodRows;
               }
               break label115;
            }

            this.saveOutOfOrderRow(prevKey, key, iterator);
            var7 = false;
         } catch (Throwable var17) {
            var6 = var17;
            throw var17;
         } finally {
            if(iterator != null) {
               if(var6 != null) {
                  try {
                     iterator.close();
                  } catch (Throwable var16) {
                     var6.addSuppressed(var16);
                  }
               } else {
                  iterator.close();
               }
            }

         }

         return var7;
      }

      if(sstableIterator.hasRowsOutOfOrder()) {
         this.outputHandler.warn(String.format("Out of order rows found in partition: %s", new Object[]{key}));
         this.outOfOrder.add(sstableIterator.getRowsOutOfOrder());
      }

      return true;
   }

   private UnfilteredRowIterator getIterator(DecoratedKey key) {
      Scrubber.RowMergingSSTableIterator rowMergingIterator = new Scrubber.RowMergingSSTableIterator(SSTableIdentityIterator.create(this.sstable, this.dataFile, key));
      return (UnfilteredRowIterator)(this.reinsertOverflowedTTLRows?new Scrubber.FixNegativeLocalDeletionTimeIterator(rowMergingIterator, this.outputHandler, this.negativeLocalDeletionInfoMetrics):rowMergingIterator);
   }

   private void seekToNextRow() {
      long nextRowPositionFromIndex = this.indexIterator.dataPosition();
      if(nextRowPositionFromIndex == -1L) {
         nextRowPositionFromIndex = this.dataFile.length();
      }

      try {
         this.dataFile.seek(nextRowPositionFromIndex);
      } catch (Throwable var4) {
         this.throwIfFatal(var4);
         this.outputHandler.warn(String.format("Failed to seek to next row position %d", new Object[]{Long.valueOf(nextRowPositionFromIndex)}), var4);
      }

   }

   private void saveOutOfOrderRow(DecoratedKey prevKey, DecoratedKey key, UnfilteredRowIterator iterator) {
      this.outputHandler.warn(String.format("Out of order row detected (%s found after %s)", new Object[]{key, prevKey}));
      this.outOfOrder.add(ArrayBackedPartition.create(iterator));
   }

   private void throwIfFatal(Throwable th) {
      if(th instanceof Error && !(th instanceof AssertionError) && !(th instanceof IOError)) {
         throw (Error)th;
      }
   }

   private void throwIfCannotContinue(DecoratedKey key, Throwable th) {
      if(this.isIndex) {
         this.outputHandler.warn(String.format("An error occurred while scrubbing the row with key '%s' for an index table. Scrubbing will abort for this table and the index will be rebuilt.", new Object[]{key}));
         throw new IOError(th);
      } else if(this.isCommutative && !this.skipCorrupted) {
         this.outputHandler.warn(String.format("An error occurred while scrubbing the row with key '%s'.  Skipping corrupt rows in counter tables will result in undercounts for the affected counters (see CASSANDRA-2759 for more details), so by default the scrub will stop at this point.  If you would like to skip the row anyway and continue scrubbing, re-run the scrub with the --skip-corrupted option.", new Object[]{key}));
         throw new IOError(th);
      }
   }

   public void close() {
      FileUtils.closeQuietly((Closeable)this.indexIterator);
      FileUtils.closeQuietly((Closeable)this.dataFile);
   }

   public CompactionInfo.Holder getScrubInfo() {
      return this.scrubInfo;
   }

   @VisibleForTesting
   public Scrubber.ScrubResult scrubWithResult() {
      this.scrub();
      return new Scrubber.ScrubResult(this);
   }

   private static final class FixNegativeLocalDeletionTimeIterator extends AbstractIterator<Unfiltered> implements UnfilteredRowIterator {
      private final UnfilteredRowIterator iterator;
      private final OutputHandler outputHandler;
      private final Scrubber.NegativeLocalDeletionInfoMetrics negativeLocalExpirationTimeMetrics;

      public FixNegativeLocalDeletionTimeIterator(UnfilteredRowIterator iterator, OutputHandler outputHandler, Scrubber.NegativeLocalDeletionInfoMetrics negativeLocalDeletionInfoMetrics) {
         this.iterator = iterator;
         this.outputHandler = outputHandler;
         this.negativeLocalExpirationTimeMetrics = negativeLocalDeletionInfoMetrics;
      }

      public TableMetadata metadata() {
         return this.iterator.metadata();
      }

      public boolean isReverseOrder() {
         return this.iterator.isReverseOrder();
      }

      public RegularAndStaticColumns columns() {
         return this.iterator.columns();
      }

      public DecoratedKey partitionKey() {
         return this.iterator.partitionKey();
      }

      public Row staticRow() {
         return this.iterator.staticRow();
      }

      public boolean isEmpty() {
         return this.iterator.isEmpty();
      }

      public void close() {
         this.iterator.close();
      }

      public DeletionTime partitionLevelDeletion() {
         return this.iterator.partitionLevelDeletion();
      }

      public EncodingStats stats() {
         return this.iterator.stats();
      }

      protected Unfiltered computeNext() {
         if(!this.iterator.hasNext()) {
            return (Unfiltered)this.endOfData();
         } else {
            Unfiltered next = (Unfiltered)this.iterator.next();
            if(!next.isRow()) {
               return next;
            } else if(this.hasNegativeLocalExpirationTime((Row)next)) {
               this.outputHandler.debug(String.format("Found row with negative local expiration time: %s", new Object[]{next.toString(this.metadata(), false)}));
               ++this.negativeLocalExpirationTimeMetrics.fixedRows;
               return this.fixNegativeLocalExpirationTime((Row)next);
            } else {
               return next;
            }
         }
      }

      private boolean hasNegativeLocalExpirationTime(Row next) {
         Row row = next;
         if (row.primaryKeyLivenessInfo().isExpiring() && row.primaryKeyLivenessInfo().localExpirationTime() < 0) {
            return true;
         }
         for (ColumnData cd : row) {
            if (cd.column().isSimple()) {
               Cell cell = (Cell)cd;
               if (!cell.isExpiring() || cell.localDeletionTime() >= 0) continue;
               return true;
            }
            ComplexColumnData complexData = (ComplexColumnData)cd;
            for (Cell cell : complexData) {
               if (!cell.isExpiring() || cell.localDeletionTime() >= 0) continue;
               return true;
            }
         }
         return false;
      }

      private Unfiltered fixNegativeLocalExpirationTime(Row row) {
         Row.Builder builder = HeapAllocator.instance.cloningRowBuilder();
         builder.newRow(row.clustering());
         builder.addPrimaryKeyLivenessInfo(row.primaryKeyLivenessInfo().isExpiring() && row.primaryKeyLivenessInfo().localExpirationTime() < 0?row.primaryKeyLivenessInfo().withUpdatedTimestampAndLocalDeletionTime(row.primaryKeyLivenessInfo().timestamp() + 1L, 2147483646):row.primaryKeyLivenessInfo());
         builder.addRowDeletion(row.deletion());
         Iterator var3 = row.iterator();

         while(true) {
            while(var3.hasNext()) {
               ColumnData cd = (ColumnData)var3.next();
               if(cd.column().isSimple()) {
                  Cell cell = (Cell)cd;
                  builder.addCell(cell.isExpiring() && cell.localDeletionTime() < 0?cell.withUpdatedTimestampAndLocalDeletionTime(cell.timestamp() + 1L, 2147483646):cell);
               } else {
                  ComplexColumnData complexData = (ComplexColumnData)cd;
                  builder.addComplexDeletion(complexData.column(), complexData.complexDeletion());
                  Iterator var6 = complexData.iterator();

                  while(var6.hasNext()) {
                     Cell cell = (Cell)var6.next();
                     builder.addCell(cell.isExpiring() && cell.localDeletionTime() < 0?cell.withUpdatedTimestampAndLocalDeletionTime(cell.timestamp() + 1L, 2147483646):cell);
                  }
               }
            }

            return builder.build();
         }
      }
   }

   private static final class OrderCheckerIterator extends AbstractIterator<Unfiltered> implements UnfilteredRowIterator {
      private final UnfilteredRowIterator iterator;
      private final ClusteringComparator comparator;
      private Unfiltered previous;
      private Partition rowsOutOfOrder;

      public OrderCheckerIterator(UnfilteredRowIterator iterator, ClusteringComparator comparator) {
         this.iterator = iterator;
         this.comparator = comparator;
      }

      public TableMetadata metadata() {
         return this.iterator.metadata();
      }

      public boolean isReverseOrder() {
         return this.iterator.isReverseOrder();
      }

      public RegularAndStaticColumns columns() {
         return this.iterator.columns();
      }

      public DecoratedKey partitionKey() {
         return this.iterator.partitionKey();
      }

      public Row staticRow() {
         return this.iterator.staticRow();
      }

      public boolean isEmpty() {
         return this.iterator.isEmpty();
      }

      public void close() {
         this.iterator.close();
      }

      public DeletionTime partitionLevelDeletion() {
         return this.iterator.partitionLevelDeletion();
      }

      public EncodingStats stats() {
         return this.iterator.stats();
      }

      public boolean hasRowsOutOfOrder() {
         return this.rowsOutOfOrder != null;
      }

      public Partition getRowsOutOfOrder() {
         return this.rowsOutOfOrder;
      }

      protected Unfiltered computeNext() {
         if(!this.iterator.hasNext()) {
            return (Unfiltered)this.endOfData();
         } else {
            Unfiltered next = (Unfiltered)this.iterator.next();
            if(this.previous != null && this.comparator.compare((Clusterable)next, (Clusterable)this.previous) < 0) {
               this.rowsOutOfOrder = ImmutableBTreePartition.create(UnfilteredRowIterators.concat(next, this.iterator), false);
               return (Unfiltered)this.endOfData();
            } else {
               this.previous = next;
               return next;
            }
         }
      }
   }

   private static class RowMergingSSTableIterator extends WrappingUnfilteredRowIterator {
      Unfiltered nextToOffer = null;

      RowMergingSSTableIterator(UnfilteredRowIterator source) {
         super(source);
      }

      public boolean hasNext() {
         return this.nextToOffer != null || this.wrapped.hasNext();
      }

      public Unfiltered next() {
         Unfiltered next = this.nextToOffer != null?this.nextToOffer:(Unfiltered)this.wrapped.next();
         if(((Unfiltered)next).isRow()) {
            while(this.wrapped.hasNext()) {
               Unfiltered peek = (Unfiltered)this.wrapped.next();
               if(!peek.isRow() || !((Unfiltered)next).clustering().equals(peek.clustering())) {
                  this.nextToOffer = peek;
                  return (Unfiltered)next;
               }

               next = Rows.merge((Row)next, (Row)peek, FBUtilities.nowInSeconds());
            }
         }

         this.nextToOffer = null;
         return (Unfiltered)next;
      }
   }

   public class NegativeLocalDeletionInfoMetrics {
      public volatile int fixedRows = 0;

      public NegativeLocalDeletionInfoMetrics() {
      }
   }

   public static final class ScrubResult {
      public final int goodRows;
      public final int badRows;
      public final int emptyRows;

      public ScrubResult(Scrubber scrubber) {
         this.goodRows = scrubber.goodRows;
         this.badRows = scrubber.badRows;
         this.emptyRows = scrubber.emptyRows;
      }
   }

   private static class ScrubInfo extends CompactionInfo.Holder {
      private final RandomAccessReader dataFile;
      private final SSTableReader sstable;
      private final UUID scrubCompactionId;

      public ScrubInfo(RandomAccessReader dataFile, SSTableReader sstable) {
         this.dataFile = dataFile;
         this.sstable = sstable;
         this.scrubCompactionId = UUIDGen.getTimeUUID();
      }

      public CompactionInfo getCompactionInfo() {
         try {
            return new CompactionInfo(this.sstable.metadata(), OperationType.SCRUB, this.dataFile.getFilePointer(), this.dataFile.length(), this.scrubCompactionId);
         } catch (Exception var2) {
            throw new RuntimeException(var2);
         }
      }
   }
}
