package org.apache.cassandra.index.sasi.disk;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.sasi.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.utils.CombinedTermIterator;
import org.apache.cassandra.index.sasi.utils.TypeUtil;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerSSTableIndexWriter implements SSTableFlushObserver {
   private static final Logger logger = LoggerFactory.getLogger(PerSSTableIndexWriter.class);
   private static final int POOL_SIZE = 8;
   private static final ThreadPoolExecutor INDEX_FLUSHER_MEMTABLE;
   private static final ThreadPoolExecutor INDEX_FLUSHER_GENERAL;
   private final int nowInSec = FBUtilities.nowInSeconds();
   private final org.apache.cassandra.io.sstable.Descriptor descriptor;
   private final OperationType source;
   private final AbstractType<?> keyValidator;
   @VisibleForTesting
   protected final Multimap<ColumnMetadata, PerSSTableIndexWriter.Index> indexes;
   private DecoratedKey currentKey;
   private long currentKeyPosition;
   private boolean isComplete;

   public PerSSTableIndexWriter(AbstractType<?> keyValidator, org.apache.cassandra.io.sstable.Descriptor descriptor, OperationType source, Multimap<ColumnMetadata, ColumnIndex> supportedIndexes) {
      this.keyValidator = keyValidator;
      this.descriptor = descriptor;
      this.source = source;
      this.indexes = HashMultimap.create();
      Iterator var5 = supportedIndexes.entries().iterator();

      while(var5.hasNext()) {
         Entry<ColumnMetadata, ColumnIndex> entry = (Entry)var5.next();
         this.indexes.put(entry.getKey(), this.newIndex((ColumnIndex)entry.getValue()));
      }

   }

   public void begin() {
   }

   public void startPartition(DecoratedKey key, long curPosition) {
      this.currentKey = key;
      this.currentKeyPosition = curPosition;
   }

   public void nextUnfilteredCluster(Unfiltered unfiltered) {
      if(unfiltered.isRow()) {
         Row row = (Row)unfiltered;
         this.indexes.entries().forEach((entry) -> {
            ByteBuffer value = ColumnIndex.getValueOf((ColumnMetadata)entry.getKey(), row, this.nowInSec);
            if(value != null) {
               PerSSTableIndexWriter.Index index = (PerSSTableIndexWriter.Index)entry.getValue();
               index.add(value.duplicate(), this.currentKey, this.currentKeyPosition);
            }
         });
      }
   }

   public void complete() {
      if(!this.isComplete) {
         this.currentKey = null;

         try {
            CountDownLatch latch = new CountDownLatch(this.indexes.size());
            Iterator var2 = this.indexes.values().iterator();

            while(var2.hasNext()) {
               PerSSTableIndexWriter.Index index = (PerSSTableIndexWriter.Index)var2.next();
               index.complete(latch);
            }

            Uninterruptibles.awaitUninterruptibly(latch);
         } finally {
            this.indexes.clear();
            this.isComplete = true;
         }
      }
   }

   @VisibleForTesting
   public List<PerSSTableIndexWriter.Index> getIndexes(ColumnMetadata columnMetadata) {
      return new ArrayList(this.indexes.get(columnMetadata));
   }

   public org.apache.cassandra.io.sstable.Descriptor getDescriptor() {
      return this.descriptor;
   }

   @VisibleForTesting
   protected PerSSTableIndexWriter.Index newIndex(ColumnIndex columnIndex) {
      return new PerSSTableIndexWriter.Index(columnIndex, this.descriptor);
   }

   protected long maxMemorySize(ColumnIndex columnIndex) {
      return this.source == OperationType.FLUSH?1073741824L:columnIndex.getMode().maxCompactionFlushMemoryInMb;
   }

   public int hashCode() {
      return this.descriptor.hashCode();
   }

   public boolean equals(Object o) {
      return o != null && o instanceof PerSSTableIndexWriter && this.descriptor.equals(((PerSSTableIndexWriter)o).descriptor);
   }

   static {
      INDEX_FLUSHER_GENERAL = new JMXEnabledThreadPoolExecutor(8, 8, 1L, TimeUnit.MINUTES, new LinkedBlockingQueue(), new NamedThreadFactory("SASI-General"), "internal");
      INDEX_FLUSHER_GENERAL.allowCoreThreadTimeOut(true);
      INDEX_FLUSHER_MEMTABLE = new JMXEnabledThreadPoolExecutor(8, 8, 1L, TimeUnit.MINUTES, new LinkedBlockingQueue(), new NamedThreadFactory("SASI-Memtable"), "internal");
      INDEX_FLUSHER_MEMTABLE.allowCoreThreadTimeOut(true);
   }

   @VisibleForTesting
   protected class Index {
      @VisibleForTesting
      protected final String outputFile;
      private final ColumnIndex columnIndex;
      private final AbstractAnalyzer analyzer;
      private final org.apache.cassandra.io.sstable.Descriptor descriptor;
      private final long maxMemorySize;
      @VisibleForTesting
      protected final Set<Future<OnDiskIndex>> segments;
      private int segmentNumber;
      private OnDiskIndexBuilder currentBuilder;

      private Index(ColumnIndex columnIndex, org.apache.cassandra.io.sstable.Descriptor descriptor) {
         this.segmentNumber = 0;
         this.columnIndex = columnIndex;
         this.descriptor = descriptor;
         this.outputFile = descriptor.filenameFor(columnIndex.getComponent());
         this.analyzer = columnIndex.getAnalyzer();
         this.segments = new HashSet();
         this.maxMemorySize = PerSSTableIndexWriter.this.maxMemorySize(columnIndex);
         this.currentBuilder = this.newIndexBuilder();
      }

      public void add(ByteBuffer term, DecoratedKey key, long keyPosition) {
         if(term.remaining() != 0) {
            boolean isAdded = false;
            this.analyzer.reset(term);

            while(true) {
               while(this.analyzer.hasNext()) {
                  ByteBuffer token = this.analyzer.next();
                  int size = token.remaining();
                  if(token.remaining() >= 1024) {
                     PerSSTableIndexWriter.logger.info("Rejecting value (size {}, maximum {}) for column {} (analyzed {}) at {} SSTable.", new Object[]{FBUtilities.prettyPrintMemory((long)term.remaining()), FBUtilities.prettyPrintMemory(1024L), this.columnIndex.getColumnName(), Boolean.valueOf(this.columnIndex.getMode().isAnalyzed), this.descriptor});
                  } else if(!TypeUtil.isValid(token, this.columnIndex.getValidator()) && (token = TypeUtil.tryUpcast(token, this.columnIndex.getValidator())) == null) {
                     PerSSTableIndexWriter.logger.info("({}) Failed to add {} to index for key: {}, value size was {}, validator is {}.", new Object[]{this.outputFile, this.columnIndex.getColumnName(), PerSSTableIndexWriter.this.keyValidator.getString(key.getKey()), FBUtilities.prettyPrintMemory((long)size), this.columnIndex.getValidator()});
                  } else {
                     this.currentBuilder.add(token, key, keyPosition);
                     isAdded = true;
                  }
               }

               if(isAdded && this.currentBuilder.estimatedMemoryUse() >= this.maxMemorySize) {
                  this.segments.add(this.getExecutor().submit(this.scheduleSegmentFlush(false)));
                  return;
               }

               return;
            }
         }
      }

      @VisibleForTesting
      protected Callable<OnDiskIndex> scheduleSegmentFlush(boolean isFinal) {
         OnDiskIndexBuilder builder = this.currentBuilder;
         this.currentBuilder = this.newIndexBuilder();
         String segmentFile = this.filename(isFinal);
         return () -> {
            long start = System.nanoTime();

            OnDiskIndex var7;
            try {
               File index = new File(segmentFile);
               var7 = builder.finish(index)?new OnDiskIndex(index, this.columnIndex.getValidator(), (Function)null):null;
               return var7;
            } catch (FSError | Exception var11) {
               PerSSTableIndexWriter.logger.error("Failed to build index segment {}", segmentFile, var11);
               var7 = null;
            } finally {
               if(!isFinal) {
                  PerSSTableIndexWriter.logger.info("Flushed index segment {}, took {} ms.", segmentFile, Long.valueOf(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)));
               }

            }

            return var7;
         };
      }

      public void complete(CountDownLatch latch) {
         PerSSTableIndexWriter.logger.info("Scheduling index flush to {}", this.outputFile);
         this.getExecutor().submit(() -> {
            long start1 = System.nanoTime();
            OnDiskIndex[] parts = new OnDiskIndex[this.segments.size() + 1];
            boolean var16 = false;

            int segmentx;
            OnDiskIndex partx;
            label259: {
               label260: {
                  try {
                     var16 = true;
                     if(this.segments.isEmpty()) {
                        this.scheduleSegmentFlush(true).call();
                        var16 = false;
                        break label259;
                     }

                     if(!this.currentBuilder.isEmpty()) {
                        OnDiskIndex last = (OnDiskIndex)this.scheduleSegmentFlush(false).call();
                        this.segments.add(Futures.immediateFuture(last));
                     }

                     segmentx = 0;
                     ByteBuffer combinedMin = null;
                     ByteBuffer combinedMax = null;
                     Iterator var8 = this.segments.iterator();

                     while(var8.hasNext()) {
                        Future<OnDiskIndex> f = (Future)var8.next();
                        OnDiskIndex partxx = (OnDiskIndex)f.get();
                        if(partxx != null) {
                           parts[segmentx++] = partxx;
                           combinedMin = combinedMin != null && PerSSTableIndexWriter.this.keyValidator.compare(combinedMin, partxx.minKey()) <= 0?combinedMin:partxx.minKey();
                           combinedMax = combinedMax != null && PerSSTableIndexWriter.this.keyValidator.compare(combinedMax, partxx.maxKey()) >= 0?combinedMax:partxx.maxKey();
                        }
                     }

                     OnDiskIndexBuilder builder = this.newIndexBuilder();
                     builder.finish(Pair.create(combinedMin, combinedMax), new File(this.outputFile), new CombinedTermIterator(parts));
                     var16 = false;
                     break label260;
                  } catch (FSError | Exception var17) {
                     PerSSTableIndexWriter.logger.error("Failed to flush index {}.", this.outputFile, var17);
                     FileUtils.delete(this.outputFile);
                     var16 = false;
                  } finally {
                     if(var16) {
                        PerSSTableIndexWriter.logger.info("Index flush to {} took {} ms.", this.outputFile, Long.valueOf(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start1)));
                        int segment = 0;

                        while(true) {
                           if(segment >= this.segmentNumber) {
                              latch.countDown();
                           } else {
                              OnDiskIndex part = parts[segment];
                              if(part != null) {
                                 FileUtils.closeQuietly((Closeable)part);
                              }

                              FileUtils.delete(this.outputFile + "_" + segment);
                              ++segment;
                           }
                        }
                     }
                  }

                  PerSSTableIndexWriter.logger.info("Index flush to {} took {} ms.", this.outputFile, Long.valueOf(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start1)));

                  for(segmentx = 0; segmentx < this.segmentNumber; ++segmentx) {
                     partx = parts[segmentx];
                     if(partx != null) {
                        FileUtils.closeQuietly((Closeable)partx);
                     }

                     FileUtils.delete(this.outputFile + "_" + segmentx);
                  }

                  latch.countDown();
                  return;
               }

               PerSSTableIndexWriter.logger.info("Index flush to {} took {} ms.", this.outputFile, Long.valueOf(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start1)));

               for(segmentx = 0; segmentx < this.segmentNumber; ++segmentx) {
                  partx = parts[segmentx];
                  if(partx != null) {
                     FileUtils.closeQuietly((Closeable)partx);
                  }

                  FileUtils.delete(this.outputFile + "_" + segmentx);
               }

               latch.countDown();
               return;
            }

            PerSSTableIndexWriter.logger.info("Index flush to {} took {} ms.", this.outputFile, Long.valueOf(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start1)));

            for(segmentx = 0; segmentx < this.segmentNumber; ++segmentx) {
               partx = parts[segmentx];
               if(partx != null) {
                  FileUtils.closeQuietly((Closeable)partx);
               }

               FileUtils.delete(this.outputFile + "_" + segmentx);
            }

            latch.countDown();
         });
      }

      private ExecutorService getExecutor() {
         return PerSSTableIndexWriter.this.source == OperationType.FLUSH?PerSSTableIndexWriter.INDEX_FLUSHER_MEMTABLE:PerSSTableIndexWriter.INDEX_FLUSHER_GENERAL;
      }

      private OnDiskIndexBuilder newIndexBuilder() {
         return new OnDiskIndexBuilder(PerSSTableIndexWriter.this.keyValidator, this.columnIndex.getValidator(), this.columnIndex.getMode().mode);
      }

      public String filename(boolean isFinal) {
         return this.outputFile + (isFinal?"":"_" + this.segmentNumber++);
      }

      public boolean equals(Object obj) {
         if(obj == this) {
            return true;
         } else if(!(obj instanceof PerSSTableIndexWriter.Index)) {
            return false;
         } else {
            PerSSTableIndexWriter.Index other = (PerSSTableIndexWriter.Index)obj;
            return Objects.equals(this.columnIndex, other.columnIndex) && Objects.equals(this.descriptor, other.descriptor);
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.columnIndex, this.descriptor});
      }
   }
}
