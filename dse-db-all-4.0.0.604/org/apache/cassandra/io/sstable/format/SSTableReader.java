package org.apache.cassandra.io.sstable.format;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.RateLimiter;
import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.mos.MemoryLockedBuffer;
import org.apache.cassandra.db.mos.MemoryOnlyStatus;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.BloomFilterTracker;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.AsynchronousChannelProxy;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.FileAccessType;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.AlwaysPresentFilter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SelfRefCounted;
import org.apache.cassandra.utils.flow.Flow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SSTableReader extends SSTable implements SelfRefCounted<SSTableReader> {
   private static final Logger logger = LoggerFactory.getLogger(SSTableReader.class);
   public static final ScheduledThreadPoolExecutor readHotnessTrackerExecutor = initSyncExecutor();
   private static final RateLimiter meterSyncThrottle = RateLimiter.create(100.0D);
   public static final Comparator<SSTableReader> maxTimestampComparator = (o1, o2) -> {
      return Long.compare(o2.getMaxTimestamp(), o1.getMaxTimestamp());
   };
   public static final Comparator<SSTableReader> sstableComparator = (o1, o2) -> {
      return o1.first.compareTo((PartitionPosition)o2.first);
   };
   public static final Comparator<SSTableReader> generationReverseComparator = (o1, o2) -> {
      return -Integer.compare(o1.descriptor.generation, o2.descriptor.generation);
   };
   public static final Ordering<SSTableReader> sstableOrdering;
   public static final Comparator<SSTableReader> sizeComparator;
   public final long maxDataAge;
   public final SSTableReader.OpenReason openReason;
   public final SSTableReader.UniqueIdentifier instanceId = new SSTableReader.UniqueIdentifier();
   protected FileHandle dataFile;
   protected IFilter bf;
   protected final BloomFilterTracker bloomFilterTracker = new BloomFilterTracker();
   protected final AtomicBoolean isSuspect = new AtomicBoolean(false);
   protected volatile StatsMetadata sstableMetadata;
   protected final EncodingStats stats;
   public final SerializationHeader header;
   protected final SSTableReader.InstanceTidier tidy;
   private final Ref<SSTableReader> selfRef;
   private RestorableMeter readMeter;
   private volatile double crcCheckChance;

   private static ScheduledThreadPoolExecutor initSyncExecutor() {
      if(DatabaseDescriptor.isClientOrToolInitialized()) {
         return null;
      } else {
         ScheduledThreadPoolExecutor syncExecutor = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("read-hotness-tracker"));
         syncExecutor.setRemoveOnCancelPolicy(true);
         return syncExecutor;
      }
   }

   public static long getApproximateKeyCount(Iterable<SSTableReader> sstables) {
      long count = -1L;
      if(Iterables.isEmpty(sstables)) {
         return count;
      } else {
         boolean failed = false;
         ICardinality cardinality = null;
         Iterator var5 = sstables.iterator();

         SSTableReader sstable;
         while(var5.hasNext()) {
            sstable = (SSTableReader)var5.next();
            if(sstable.openReason != SSTableReader.OpenReason.EARLY) {
               try {
                  CompactionMetadata metadata = (CompactionMetadata)sstable.descriptor.getMetadataSerializer().deserialize(sstable.descriptor, MetadataType.COMPACTION);
                  if(metadata == null) {
                     logger.warn("Reading cardinality from Statistics.db failed for {}", sstable.getFilename());
                     failed = true;
                     break;
                  }

                  if(cardinality == null) {
                     cardinality = metadata.cardinalityEstimator;
                  } else {
                     cardinality = cardinality.merge(new ICardinality[]{metadata.cardinalityEstimator});
                  }
               } catch (IOException var8) {
                  logger.warn("Reading cardinality from Statistics.db failed.", var8);
                  failed = true;
                  break;
               } catch (CardinalityMergeException var9) {
                  logger.warn("Cardinality merge failed.", var9);
                  failed = true;
                  break;
               }
            }
         }

         if(cardinality != null && !failed) {
            count = cardinality.cardinality();
         }

         if(count < 0L) {
            for(var5 = sstables.iterator(); var5.hasNext(); count += sstable.estimatedKeys()) {
               sstable = (SSTableReader)var5.next();
            }
         }

         return count;
      }
   }

   public static double estimateCompactionGain(Set<SSTableReader> overlapping) {
      Set<ICardinality> cardinalities = new HashSet(overlapping.size());
      Iterator var2 = overlapping.iterator();

      while(var2.hasNext()) {
         SSTableReader sstable = (SSTableReader)var2.next();

         try {
            ICardinality cardinality = ((CompactionMetadata)sstable.descriptor.getMetadataSerializer().deserialize(sstable.descriptor, MetadataType.COMPACTION)).cardinalityEstimator;
            if(cardinality != null) {
               cardinalities.add(cardinality);
            } else {
               logger.trace("Got a null cardinality estimator in: {}", sstable.getFilename());
            }
         } catch (IOException var6) {
            logger.warn("Could not read up compaction metadata for {}", sstable, var6);
         }
      }

      long totalKeyCountBefore = 0L;

      ICardinality cardinality;
      for(Iterator var8 = cardinalities.iterator(); var8.hasNext(); totalKeyCountBefore += cardinality.cardinality()) {
         cardinality = (ICardinality)var8.next();
      }

      if(totalKeyCountBefore == 0L) {
         return 1.0D;
      } else {
         long totalKeyCountAfter = mergeCardinalities(cardinalities).cardinality();
         logger.trace("Estimated compaction gain: {}/{}={}", new Object[]{Long.valueOf(totalKeyCountAfter), Long.valueOf(totalKeyCountBefore), Double.valueOf((double)totalKeyCountAfter / (double)totalKeyCountBefore)});
         return (double)totalKeyCountAfter / (double)totalKeyCountBefore;
      }
   }

   private static ICardinality mergeCardinalities(Collection<ICardinality> cardinalities) {
      Object base = new HyperLogLogPlus(13, 25);

      try {
         base = ((ICardinality)base).merge((ICardinality[])cardinalities.toArray(new ICardinality[0]));
      } catch (CardinalityMergeException var3) {
         logger.warn("Could not merge cardinalities", var3);
      }

      return (ICardinality)base;
   }

   public static SSTableReader open(Descriptor descriptor) {
      TableMetadataRef metadata;
      if(descriptor.cfname.contains(".")) {
         int i = descriptor.cfname.indexOf(".");
         String indexName = descriptor.cfname.substring(i + 1);
         metadata = Schema.instance.getIndexTableMetadataRef(descriptor.ksname, indexName);
         if(metadata == null) {
            throw new AssertionError("Could not find index metadata for index cf " + i);
         }
      } else {
         metadata = Schema.instance.getTableMetadataRef(descriptor.ksname, descriptor.cfname);
      }

      return open(descriptor, metadata);
   }

   public static SSTableReader open(Descriptor desc, TableMetadataRef metadata) {
      return open(desc, componentsFor(desc), metadata);
   }

   public static SSTableReader open(Descriptor descriptor, Set<Component> components, TableMetadataRef metadata) {
      return open(descriptor, components, metadata, true, true);
   }

   public static SSTableReader openNoValidation(Descriptor descriptor, Set<Component> components, ColumnFamilyStore cfs) {
      return open(descriptor, components, cfs.metadata, false, false);
   }

   public static SSTableReader openNoValidation(Descriptor descriptor, TableMetadataRef metadata) {
      return open(descriptor, componentsFor(descriptor), metadata, false, false);
   }

   public static SSTableReader openForBatch(Descriptor descriptor, Set<Component> components, TableMetadataRef metadata) {
      checkRequiredComponents(descriptor, components, true);
      EnumSet types = EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER);

      Map sstableMetadata;
      try {
         sstableMetadata = descriptor.getMetadataSerializer().deserialize(descriptor, types);
      } catch (IOException var26) {
         throw new CorruptSSTableException(var26, descriptor.filenameFor(Component.STATS));
      }

      ValidationMetadata validationMetadata = (ValidationMetadata)sstableMetadata.get(MetadataType.VALIDATION);
      StatsMetadata statsMetadata = (StatsMetadata)sstableMetadata.get(MetadataType.STATS);
      SerializationHeader.Component header = (SerializationHeader.Component)sstableMetadata.get(MetadataType.HEADER);
      String partitionerName = metadata.get().partitioner.getClass().getCanonicalName();
      if(validationMetadata != null && !partitionerName.equals(validationMetadata.partitioner)) {
         logger.error("Cannot open {}; partitioner {} does not match system partitioner {}.  Note that the default partitioner starting with Cassandra 1.2 is Murmur3Partitioner, so you will need to edit that to match your old partitioner if upgrading.", new Object[]{descriptor, validationMetadata.partitioner, partitionerName});
         System.exit(1);
      }

      long fileLength = (new File(descriptor.filenameFor(Component.DATA))).length();
      logger.debug("Opening {} ({})", descriptor, FBUtilities.prettyPrintMemory(fileLength));
      SSTableReader sstable = internalOpen(descriptor, components, metadata, Long.valueOf(System.currentTimeMillis()), statsMetadata, SSTableReader.OpenReason.NORMAL, header.toHeader(metadata.get()));

      try {
         FileHandle.Builder dbuilder = sstable.dataFileHandleBuilder();
         Throwable var13 = null;

         SSTableReader var14;
         try {
            sstable.bf = FilterFactory.AlwaysPresent;
            sstable.loadIndex(false);
            sstable.dataFile = dbuilder.complete();
            sstable.setup(false);
            var14 = sstable;
         } catch (Throwable var25) {
            var13 = var25;
            throw var25;
         } finally {
            if(dbuilder != null) {
               if(var13 != null) {
                  try {
                     dbuilder.close();
                  } catch (Throwable var24) {
                     var13.addSuppressed(var24);
                  }
               } else {
                  dbuilder.close();
               }
            }

         }

         return var14;
      } catch (IOException var28) {
         throw new CorruptSSTableException(var28, sstable.getFilename());
      }
   }

   public static void checkRequiredComponents(Descriptor descriptor, Set<Component> components, boolean validate) {
      if(validate) {
         assert components.containsAll(requiredComponents(descriptor)) : "Required components " + Sets.difference(requiredComponents(descriptor), components) + " missing for sstable " + descriptor;
      } else {
         assert components.contains(Component.DATA);
      }

   }

   public static Set<Component> requiredComponents(Descriptor descriptor) {
      return descriptor.getFormat().getReaderFactory().requiredComponents();
   }

   public static SSTableReader open(Descriptor descriptor, Set<Component> components, TableMetadataRef metadata, boolean validate, boolean trackHotness) {
      checkRequiredComponents(descriptor, components, validate);
      EnumSet types = EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER);

      Map sstableMetadata;
      try {
         sstableMetadata = descriptor.getMetadataSerializer().deserialize(descriptor, types);
      } catch (Throwable var17) {
         throw new CorruptSSTableException(var17, descriptor.filenameFor(Component.STATS));
      }

      ValidationMetadata validationMetadata = (ValidationMetadata)sstableMetadata.get(MetadataType.VALIDATION);
      StatsMetadata statsMetadata = (StatsMetadata)sstableMetadata.get(MetadataType.STATS);
      SerializationHeader.Component header = (SerializationHeader.Component)sstableMetadata.get(MetadataType.HEADER);

      assert header != null;

      String partitionerName = metadata.get().partitioner.getClass().getCanonicalName();
      if(validationMetadata != null && !partitionerName.equals(validationMetadata.partitioner)) {
         logger.error("Cannot open {}; partitioner {} does not match system partitioner {}.  Note that the default partitioner starting with Cassandra 1.2 is Murmur3Partitioner, so you will need to edit that to match your old partitioner if upgrading.", new Object[]{descriptor, validationMetadata.partitioner, partitionerName});
         System.exit(1);
      }

      long fileLength = (new File(descriptor.filenameFor(Component.DATA))).length();
      logger.debug("Opening {} ({})", descriptor, FBUtilities.prettyPrintMemory(fileLength));
      SSTableReader sstable = internalOpen(descriptor, components, metadata, Long.valueOf(System.currentTimeMillis()), statsMetadata, SSTableReader.OpenReason.NORMAL, header.toHeader(metadata.get()));

      try {
         long start = System.nanoTime();
         sstable.load(validationMetadata);
         logger.trace("INDEX LOAD TIME for {}: {} ms.", descriptor, Long.valueOf(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)));
         sstable.setup(trackHotness);
         if(validate) {
            sstable.validate();
         }

         return sstable;
      } catch (Throwable var16) {
         sstable.selfRef().release();
         throw new CorruptSSTableException(var16, sstable.getFilename());
      }
   }

   public static Collection<SSTableReader> openAll(Set<Entry<Descriptor, Set<Component>>> entries, final TableMetadataRef metadata) {
      final Collection<SSTableReader> sstables = new LinkedBlockingQueue();
      long start = System.nanoTime();
      int threadCount = FBUtilities.getAvailableProcessors();
      ExecutorService executor = DebuggableThreadPoolExecutor.createWithFixedPoolSize("SSTableBatchOpen", threadCount);
      Iterator var7 = entries.iterator();

      while(var7.hasNext()) {
         final Entry<Descriptor, Set<Component>> entry = (Entry)var7.next();
         Runnable runnable = new Runnable() {
            public void run() {
               SSTableReader sstable;
               try {
                  sstable = SSTableReader.open((Descriptor)entry.getKey(), (Set)entry.getValue(), metadata);
               } catch (CorruptSSTableException var3) {
                  FileUtils.handleCorruptSSTable(var3);
                  SSTableReader.logger.error("Corrupt sstable {}; skipping table", entry, var3);
                  return;
               } catch (FSError var4) {
                  FileUtils.handleFSError(var4);
                  SSTableReader.logger.error("Cannot read sstable {}; file system error, skipping table", entry, var4);
                  return;
               }

               sstables.add(sstable);
            }
         };
         executor.submit(runnable);
      }

      executor.shutdown();

      try {
         executor.awaitTermination(7L, TimeUnit.DAYS);
      } catch (InterruptedException var10) {
         throw new AssertionError(var10);
      }

      long timeTaken = System.nanoTime() - start;
      logger.info(String.format("openAll time for table %s using %d threads: %,.3fms", new Object[]{metadata.name, Integer.valueOf(threadCount), Double.valueOf((double)timeTaken * 1.0E-6D)}));
      return sstables;
   }

   protected static SSTableReader internalOpen(Descriptor descriptor, Set<Component> components, TableMetadataRef metadata, Long maxDataAge, StatsMetadata sstableMetadata, SSTableReader.OpenReason openReason, SerializationHeader header) {
      SSTableReader.Factory readerFactory = descriptor.getFormat().getReaderFactory();
      return readerFactory.open(descriptor, components, metadata, maxDataAge, sstableMetadata, openReason, header);
   }

   protected SSTableReader(Descriptor desc, Set<Component> components, TableMetadataRef metadata, long maxDataAge, StatsMetadata sstableMetadata, SSTableReader.OpenReason openReason, SerializationHeader header) {
      super(desc, components, metadata, DatabaseDescriptor.getDiskOptimizationStrategy());
      this.sstableMetadata = sstableMetadata;
      this.stats = new EncodingStats(sstableMetadata.minTimestamp, sstableMetadata.minLocalDeletionTime, sstableMetadata.minTTL);
      this.header = header;
      this.maxDataAge = maxDataAge;
      this.openReason = openReason;
      this.tidy = new SSTableReader.InstanceTidier(this.descriptor, metadata.id);
      this.selfRef = new Ref(this, this.tidy);
   }

   public static long getTotalBytes(Iterable<SSTableReader> sstables) {
      long sum = 0L;

      SSTableReader sstable;
      for(Iterator var3 = sstables.iterator(); var3.hasNext(); sum += sstable.onDiskLength()) {
         sstable = (SSTableReader)var3.next();
      }

      return sum;
   }

   public static long getTotalUncompressedBytes(Iterable<SSTableReader> sstables) {
      long sum = 0L;

      SSTableReader sstable;
      for(Iterator var3 = sstables.iterator(); var3.hasNext(); sum += sstable.uncompressedLength()) {
         sstable = (SSTableReader)var3.next();
      }

      return sum;
   }

   public boolean equals(Object that) {
      return that instanceof SSTableReader && ((SSTableReader)that).descriptor.equals(this.descriptor);
   }

   public int hashCode() {
      return this.descriptor.hashCode();
   }

   public String getFilename() {
      return this.dataFile.path();
   }

   public void setupOnline() {
      ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(this.metadata().id);
      if(cfs != null) {
         this.setCrcCheckChance(cfs.getCrcCheckChance().doubleValue());
      }

   }

   private void load(ValidationMetadata validation) throws IOException {
      this.load();
   }

   private void load() throws IOException {
      try {
         FileHandle.Builder dbuilder = this.dataFileHandleBuilder();
         Throwable var2 = null;

         try {
            this.loadBloomFilter();
            this.loadIndex(this.bf == FilterFactory.AlwaysPresent);
            this.dataFile = dbuilder.complete();
         } catch (Throwable var12) {
            var2 = var12;
            throw var12;
         } finally {
            if(dbuilder != null) {
               if(var2 != null) {
                  try {
                     dbuilder.close();
                  } catch (Throwable var11) {
                     var2.addSuppressed(var11);
                  }
               } else {
                  dbuilder.close();
               }
            }

         }

      } catch (Throwable var14) {
         if(this.dataFile != null) {
            this.dataFile.close();
            this.dataFile = null;
         }

         this.releaseIndex();
         throw var14;
      }
   }

   private void loadBloomFilter() throws IOException {
      if(!this.components.contains(Component.FILTER)) {
         this.bf = FilterFactory.AlwaysPresent;
      } else {
         DataInputStream stream = new DataInputStream(new BufferedInputStream(Files.newInputStream(Paths.get(this.descriptor.filenameFor(Component.FILTER), new String[0]), new OpenOption[0])));
         Throwable var2 = null;

         try {
            this.bf = FilterFactory.deserialize(stream, true);
         } catch (Throwable var11) {
            var2 = var11;
            throw var11;
         } finally {
            if(stream != null) {
               if(var2 != null) {
                  try {
                     stream.close();
                  } catch (Throwable var10) {
                     var2.addSuppressed(var10);
                  }
               } else {
                  stream.close();
               }
            }

         }

      }
   }

   protected abstract void loadIndex(boolean var1) throws IOException;

   protected abstract void releaseIndex();

   protected FileHandle.Builder indexFileHandleBuilder(Component component) {
      return indexFileHandleBuilder(this.descriptor, this.metadata(), component);
   }

   public static FileHandle.Builder indexFileHandleBuilder(Descriptor descriptor, TableMetadata metadata, Component component) {
      return (new FileHandle.Builder(descriptor.filenameFor(component))).withChunkCache(ChunkCache.instance).mmapped(metadata.indexAccessMode == Config.AccessMode.mmap).bufferSize(4096).withChunkCache(ChunkCache.instance);
   }

   public static FileHandle.Builder dataFileHandleBuilder(Descriptor descriptor, TableMetadata metadata, boolean compression) {
      return (new FileHandle.Builder(descriptor.filenameFor(Component.DATA))).compressed(compression).mmapped(metadata.diskAccessMode == Config.AccessMode.mmap).withChunkCache(ChunkCache.instance);
   }

   FileHandle.Builder dataFileHandleBuilder() {
      int dataBufferSize = this.optimizationStrategy.bufferSize(this.sstableMetadata.estimatedPartitionSize.percentile(DatabaseDescriptor.getDiskOptimizationEstimatePercentile()));
      return dataFileHandleBuilder(this.descriptor, this.metadata(), this.compression).bufferSize(dataBufferSize);
   }

   public void setReplaced() {
      synchronized(this.tidy.global) {
         assert !this.tidy.isReplaced;

         this.tidy.isReplaced = true;
      }
   }

   public boolean isReplaced() {
      synchronized(this.tidy.global) {
         return this.tidy.isReplaced;
      }
   }

   protected boolean filterFirst() {
      return this.openReason == SSTableReader.OpenReason.MOVED_START;
   }

   protected boolean filterLast() {
      return false;
   }

   private SSTableReader cloneAndReplace(DecoratedKey newFirst, SSTableReader.OpenReason reason) {
      SSTableReader replacement = this.clone(reason);
      replacement.first = newFirst;
      return replacement;
   }

   protected abstract SSTableReader clone(SSTableReader.OpenReason var1);

   public SSTableReader cloneWithRestoredStart(DecoratedKey restoredStart) {
      synchronized(this.tidy.global) {
         return this.cloneAndReplace(restoredStart, SSTableReader.OpenReason.NORMAL);
      }
   }

   public SSTableReader cloneWithNewStart(DecoratedKey newStart) {
      synchronized(this.tidy.global) {
         assert this.openReason != SSTableReader.OpenReason.EARLY;

         if(newStart.compareTo((PartitionPosition)this.first) > 0) {
            long dataStart = this.getExactPosition(newStart).position;
            this.tidy.addCloseable(new SSTableReader.DropPageCache(this.dataFile, dataStart, (FileHandle)null, 0L));
         }

         return this.cloneAndReplace(newStart, SSTableReader.OpenReason.MOVED_START);
      }
   }

   public SSTableReader cloneWithNewSummarySamplingLevel(ColumnFamilyStore parent, int samplingLevel) throws IOException {
      throw new UnsupportedOperationException();
   }

   public RestorableMeter getReadMeter() {
      return this.readMeter;
   }

   private void validate() {
      if(this.first.compareTo((PartitionPosition)this.last) > 0) {
         throw new CorruptSSTableException(new IllegalStateException(String.format("SSTable first key %s > last key %s", new Object[]{this.first, this.last})), this.getFilename());
      }
   }

   public CompressionMetadata getCompressionMetadata() {
      if(!this.compression) {
         throw new IllegalStateException(this + " is not compressed");
      } else {
         return (CompressionMetadata)this.dataFile.compressionMetadata().get();
      }
   }

   public long getCompressionMetadataOffHeapSize() {
      return !this.compression?0L:this.getCompressionMetadata().offHeapSize();
   }

   public void forceFilterFailures() {
      this.bf = FilterFactory.AlwaysPresent;
   }

   public IFilter getBloomFilter() {
      return this.bf;
   }

   public long getBloomFilterSerializedSize() {
      return this.bf.serializedSize();
   }

   public long getBloomFilterOffHeapSize() {
      return this.bf.offHeapSize();
   }

   public abstract long estimatedKeys();

   public abstract long estimatedKeysForRanges(Collection<Range<Token>> var1);

   public abstract Iterable<DecoratedKey> getKeySamples(Range<Token> var1);

   public List<Pair<Long, Long>> getPositionsForRanges(Collection<Range<Token>> ranges) {
      List<Pair<Long, Long>> positions = new ArrayList();
      Iterator var3 = Range.normalize(ranges).iterator();

      while(var3.hasNext()) {
         Range<Token> range = (Range)var3.next();

         assert !range.isTrulyWrapAround();

         AbstractBounds<PartitionPosition> bounds = Range.makeRowRange(range);
         PartitionPosition leftBound = ((PartitionPosition)bounds.left).compareTo(this.first) > 0?(PartitionPosition)bounds.left:this.first.getToken().minKeyBound();
         PartitionPosition rightBound = ((PartitionPosition)bounds.right).isMinimum()?this.last.getToken().maxKeyBound():(PartitionPosition)bounds.right;
         if(((PartitionPosition)leftBound).compareTo(this.last) <= 0 && ((PartitionPosition)rightBound).compareTo(this.first) >= 0) {
            long left = this.getPosition((PartitionPosition)leftBound, SSTableReader.Operator.GT).position;
            long right = ((PartitionPosition)rightBound).compareTo(this.last) > 0?this.uncompressedLength():this.getPosition((PartitionPosition)rightBound, SSTableReader.Operator.GT).position;
            if(left != right) {
               assert left < right : String.format("Range=%s openReason=%s first=%s last=%s left=%d right=%d", new Object[]{range, this.openReason, this.first, this.last, Long.valueOf(left), Long.valueOf(right)});

               positions.add(Pair.create(Long.valueOf(left), Long.valueOf(right)));
            }
         }
      }

      return positions;
   }

   public RowIndexEntry getPosition(PartitionPosition key, SSTableReader.Operator op) {
      return this.getPosition(key, op, SSTableReadsListener.NOOP_LISTENER, Rebufferer.ReaderConstraint.NONE);
   }

   public abstract RowIndexEntry getPosition(PartitionPosition var1, SSTableReader.Operator var2, SSTableReadsListener var3, Rebufferer.ReaderConstraint var4);

   public abstract RowIndexEntry getExactPosition(DecoratedKey var1, SSTableReadsListener var2, Rebufferer.ReaderConstraint var3);

   public abstract boolean contains(DecoratedKey var1, Rebufferer.ReaderConstraint var2);

   public RowIndexEntry getExactPosition(DecoratedKey key) {
      return this.getExactPosition(key, SSTableReadsListener.NOOP_LISTENER, Rebufferer.ReaderConstraint.NONE);
   }

   public UnfilteredRowIterator iterator(DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reversed, SSTableReadsListener listener) {
      RowIndexEntry rie = this.getExactPosition(key, listener, Rebufferer.ReaderConstraint.NONE);
      return this.iterator((FileDataInput)null, key, rie, slices, selectedColumns, reversed);
   }

   public UnfilteredRowIterator iterator(FileDataInput dataFileInput, DecoratedKey key, RowIndexEntry indexEntry, Slices slices, ColumnFilter selectedColumns, boolean reversed) {
      if(indexEntry == null) {
         return UnfilteredRowIterators.noRowsIterator(this.metadata(), key, Rows.EMPTY_STATIC_ROW, DeletionTime.LIVE, reversed);
      } else {
         boolean shouldCloseFile = false;
         if(dataFileInput == null) {
            dataFileInput = this.openDataReader();
            shouldCloseFile = true;
         }

         SerializationHelper helper = new SerializationHelper(this.metadata(), this.descriptor.version.encodingVersion(), SerializationHelper.Flag.LOCAL, selectedColumns);

         try {
            boolean needSeekAtPartitionStart = !indexEntry.isIndexed() || !selectedColumns.fetchedColumns().statics.isEmpty();
            DeletionTime partitionLevelDeletion;
            Row staticRow;
            if(needSeekAtPartitionStart) {
               ((FileDataInput)dataFileInput).seek(indexEntry.position);
               ByteBufferUtil.skipShortLength((DataInputPlus)dataFileInput);
               partitionLevelDeletion = DeletionTime.serializer.deserialize((DataInputPlus)dataFileInput);
               staticRow = readStaticRow(this, (FileDataInput)dataFileInput, helper, selectedColumns.fetchedColumns().statics);
            } else {
               partitionLevelDeletion = indexEntry.deletionTime();
               staticRow = Rows.EMPTY_STATIC_ROW;
            }

            final SSTableReader.PartitionReader reader = this.reader((FileDataInput)dataFileInput, shouldCloseFile, indexEntry, helper, slices, reversed, Rebufferer.ReaderConstraint.NONE);
            return new AbstractUnfilteredRowIterator(this.metadata(), key, partitionLevelDeletion, selectedColumns.fetchedColumns(), staticRow, reversed, this.stats()) {
               protected Unfiltered computeNext() {
                  Unfiltered next;
                  try {
                     next = reader.next();
                  } catch (IndexOutOfBoundsException | IOException var3) {
                     SSTableReader.this.markSuspect();
                     throw new CorruptSSTableException(var3, SSTableReader.this.dataFile.path());
                  }

                  return next != null?next:(Unfiltered)this.endOfData();
               }

               public void close() {
                  try {
                     reader.close();
                  } catch (IOException var2) {
                     SSTableReader.this.markSuspect();
                     throw new CorruptSSTableException(var2, SSTableReader.this.dataFile.path());
                  }
               }
            };
         } catch (IOException var14) {
            this.markSuspect();
            if(shouldCloseFile) {
               try {
                  ((FileDataInput)dataFileInput).close();
               } catch (IOException var13) {
                  var14.addSuppressed(var13);
               }
            }

            throw new CorruptSSTableException(var14, this.dataFile.path());
         }
      }
   }

   static Row readStaticRow(SSTableReader sstable, FileDataInput file, SerializationHelper helper, Columns statics) throws IOException {
      if(!sstable.header.hasStatic()) {
         return Rows.EMPTY_STATIC_ROW;
      } else if(statics.isEmpty()) {
         ((UnfilteredSerializer)UnfilteredSerializer.serializers.get(helper.version)).skipStaticRow(file, sstable.header, helper);
         return Rows.EMPTY_STATIC_ROW;
      } else {
         return ((UnfilteredSerializer)UnfilteredSerializer.serializers.get(helper.version)).deserializeStaticRow(file, sstable.header, helper);
      }
   }

   public abstract SSTableReader.PartitionReader reader(FileDataInput var1, boolean var2, RowIndexEntry var3, SerializationHelper var4, Slices var5, boolean var6, Rebufferer.ReaderConstraint var7) throws IOException;

   public abstract PartitionIndexIterator coveredKeysIterator(PartitionPosition var1, boolean var2, PartitionPosition var3, boolean var4) throws IOException;

   public abstract PartitionIndexIterator allKeysIterator() throws IOException;

   public abstract ScrubPartitionIterator scrubPartitionsIterator() throws IOException;

   public Flow<FlowableUnfilteredPartition> flow(DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reversed, SSTableReadsListener listener) {
      return AsyncPartitionReader.create(this, listener, key, slices, selectedColumns, reversed, false);
   }

   public Flow<FlowableUnfilteredPartition> flow(IndexFileEntry indexEntry, FileDataInput dfile, SSTableReadsListener listener) {
      return AsyncPartitionReader.create(this, dfile, listener, indexEntry);
   }

   public Flow<FlowableUnfilteredPartition> flow(IndexFileEntry indexEntry, FileDataInput dfile, Slices slices, ColumnFilter selectedColumns, boolean reversed, SSTableReadsListener listener) {
      return AsyncPartitionReader.create(this, dfile, listener, indexEntry, slices, selectedColumns, reversed);
   }

   public Flow<FlowableUnfilteredPartition> flowWithLowerBound(DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reversed, SSTableReadsListener listener) {
      return AsyncPartitionReader.create(this, listener, key, slices, selectedColumns, reversed, true);
   }

   public abstract Flow<IndexFileEntry> coveredKeysFlow(RandomAccessReader var1, PartitionPosition var2, boolean var3, PartitionPosition var4, boolean var5);

   public PartitionIndexIterator coveredKeysIterator(AbstractBounds<PartitionPosition> bounds) throws IOException {
      return (new SSTableReader.KeysRange(bounds)).iterator();
   }

   public Flow<IndexFileEntry> coveredKeysFlow(RandomAccessReader dataFileReader, AbstractBounds<PartitionPosition> bounds) {
      return (new SSTableReader.KeysRange(bounds)).flow(dataFileReader);
   }

   public ISSTableScanner getScanner(ColumnFilter columns, DataRange dataRange, SSTableReadsListener listener) {
      return SSTableScanner.getScanner(this, columns, dataRange, listener);
   }

   public Flow<FlowableUnfilteredPartition> getAsyncScanner(ColumnFilter columns, DataRange dataRange, SSTableReadsListener listener) {
      return AsyncSSTableScanner.getScanner(this, columns, dataRange, listener);
   }

   public ISSTableScanner getScanner(Iterator<AbstractBounds<PartitionPosition>> boundsIterator) {
      return SSTableScanner.getScanner(this, boundsIterator);
   }

   public ISSTableScanner getScanner() {
      return SSTableScanner.getScanner(this);
   }

   public Flow<FlowableUnfilteredPartition> getAsyncScanner() {
      return AsyncSSTableScanner.getScanner(this);
   }

   public Flow<FlowableUnfilteredPartition> getAsyncScanner(Collection<Range<Token>> ranges) {
      return (Flow)(ranges != null?AsyncSSTableScanner.getScanner(this, ranges):this.getAsyncScanner());
   }

   public ISSTableScanner getScanner(Collection<Range<Token>> ranges) {
      return ranges != null?SSTableScanner.getScanner(this, ranges):this.getScanner();
   }

   public UnfilteredRowIterator simpleIterator(FileDataInput dfile, DecoratedKey key, RowIndexEntry position, boolean tombstoneOnly) {
      return SSTableIdentityIterator.create(this, dfile, position, key, tombstoneOnly);
   }

   public boolean couldContain(DecoratedKey dk) {
      return !(this.bf instanceof AlwaysPresentFilter)?this.bf.isPresent(dk):this.contains(dk, Rebufferer.ReaderConstraint.NONE);
   }

   public DecoratedKey firstKeyBeyond(PartitionPosition token) {
      try {
         RowIndexEntry pos = this.getPosition(token, SSTableReader.Operator.GT);
         if(pos == null) {
            return null;
         } else {
            FileDataInput in = this.dataFile.createReader(pos.position, Rebufferer.ReaderConstraint.NONE);
            Throwable var4 = null;

            DecoratedKey var7;
            try {
               ByteBuffer indexKey = ByteBufferUtil.readWithShortLength(in);
               DecoratedKey indexDecoratedKey = this.decorateKey(indexKey);
               var7 = indexDecoratedKey;
            } catch (Throwable var17) {
               var4 = var17;
               throw var17;
            } finally {
               if(in != null) {
                  if(var4 != null) {
                     try {
                        in.close();
                     } catch (Throwable var16) {
                        var4.addSuppressed(var16);
                     }
                  } else {
                     in.close();
                  }
               }

            }

            return var7;
         }
      } catch (IOException var19) {
         this.markSuspect();
         throw new CorruptSSTableException(var19, this.dataFile.path());
      }
   }

   public long uncompressedLength() {
      return this.dataFile.dataLength();
   }

   public long onDiskLength() {
      return this.dataFile.onDiskLength;
   }

   @VisibleForTesting
   public double getCrcCheckChance() {
      return this.crcCheckChance;
   }

   public void setCrcCheckChance(double crcCheckChance) {
      this.crcCheckChance = crcCheckChance;
      this.dataFile.compressionMetadata().ifPresent((metadata) -> {
         metadata.parameters.setCrcCheckChance(crcCheckChance);
      });
   }

   public void markObsolete(Runnable tidier) {
      if(logger.isTraceEnabled()) {
         logger.trace("Marking {} compacted", this.getFilename());
      }

      synchronized(this.tidy.global) {
         assert !this.tidy.isReplaced;

         assert this.tidy.global.obsoletion == null : this + " was already marked compacted";

         this.tidy.global.obsoletion = tidier;
         this.tidy.global.stopReadMeterPersistence();
      }
   }

   public boolean isMarkedCompacted() {
      return this.tidy.global.obsoletion != null;
   }

   public void markSuspect() {
      if(logger.isTraceEnabled()) {
         logger.trace("Marking {} as a suspect for blacklisting.", this.getFilename());
      }

      this.isSuspect.getAndSet(true);
   }

   public boolean isMarkedSuspect() {
      return this.isSuspect.get();
   }

   public ISSTableScanner getScanner(Range<Token> range) {
      return range == null?this.getScanner():this.getScanner((Collection)Collections.singletonList(range));
   }

   public FileDataInput getFileDataInput(long position, Rebufferer.ReaderConstraint rc) {
      return this.dataFile.createReader(position, rc);
   }

   public boolean newSince(long age) {
      return this.maxDataAge > age;
   }

   public void createLinks(String snapshotDirectoryPath) {
      Iterator var2 = this.components.iterator();

      while(var2.hasNext()) {
         Component component = (Component)var2.next();
         File sourceFile = new File(this.descriptor.filenameFor(component));
         if(sourceFile.exists()) {
            File targetLink = new File(snapshotDirectoryPath, sourceFile.getName());
            FileUtils.createHardLink(sourceFile, targetLink);
         }
      }

   }

   public boolean isRepaired() {
      return this.sstableMetadata.repairedAt != 0L;
   }

   public abstract DecoratedKey keyAt(long var1, Rebufferer.ReaderConstraint var3) throws IOException;

   public boolean isPendingRepair() {
      return this.sstableMetadata.pendingRepair != ActiveRepairService.NO_PENDING_REPAIR;
   }

   public UUID getPendingRepair() {
      return this.sstableMetadata.pendingRepair;
   }

   public long getRepairedAt() {
      return this.sstableMetadata.repairedAt;
   }

   public boolean intersects(Collection<Range<Token>> ranges) {
      Bounds<Token> range = new Bounds(this.first.getToken(), this.last.getToken());
      return Iterables.any(ranges, (r) -> {
         return r.intersects(range);
      });
   }

   public long getBloomFilterFalsePositiveCount() {
      return this.bloomFilterTracker.getFalsePositiveCount();
   }

   public long getRecentBloomFilterFalsePositiveCount() {
      return this.bloomFilterTracker.getRecentFalsePositiveCount();
   }

   public long getBloomFilterTruePositiveCount() {
      return this.bloomFilterTracker.getTruePositiveCount();
   }

   public long getRecentBloomFilterTruePositiveCount() {
      return this.bloomFilterTracker.getRecentTruePositiveCount();
   }

   public EstimatedHistogram getEstimatedPartitionSize() {
      return this.sstableMetadata.estimatedPartitionSize;
   }

   public EstimatedHistogram getEstimatedColumnCount() {
      return this.sstableMetadata.estimatedColumnCount;
   }

   public double getEstimatedDroppableTombstoneRatio(int gcBefore) {
      return this.sstableMetadata.getEstimatedDroppableTombstoneRatio(gcBefore);
   }

   public double getDroppableTombstonesBefore(int gcBefore) {
      return this.sstableMetadata.getDroppableTombstonesBefore(gcBefore);
   }

   public double getCompressionRatio() {
      return this.sstableMetadata.compressionRatio;
   }

   public long getMinTimestamp() {
      return this.sstableMetadata.minTimestamp;
   }

   public long getMaxTimestamp() {
      return this.sstableMetadata.maxTimestamp;
   }

   public int getMinLocalDeletionTime() {
      return this.sstableMetadata.minLocalDeletionTime;
   }

   public int getMaxLocalDeletionTime() {
      return this.sstableMetadata.maxLocalDeletionTime;
   }

   public boolean mayHaveTombstones() {
      return this.getMinLocalDeletionTime() != 2147483647;
   }

   public int getMinTTL() {
      return this.sstableMetadata.minTTL;
   }

   public int getMaxTTL() {
      return this.sstableMetadata.maxTTL;
   }

   public long getTotalColumnsSet() {
      return this.sstableMetadata.totalColumnsSet;
   }

   public long getTotalRows() {
      return this.sstableMetadata.totalRows;
   }

   public int getAvgColumnSetPerRow() {
      return this.sstableMetadata.totalRows < 0L?-1:(this.sstableMetadata.totalRows == 0L?0:(int)(this.sstableMetadata.totalColumnsSet / this.sstableMetadata.totalRows));
   }

   public int getSSTableLevel() {
      return this.sstableMetadata.sstableLevel;
   }

   public void reloadSSTableMetadata() throws IOException {
      this.sstableMetadata = (StatsMetadata)this.descriptor.getMetadataSerializer().deserialize(this.descriptor, MetadataType.STATS);
   }

   public StatsMetadata getSSTableMetadata() {
      return this.sstableMetadata;
   }

   public RandomAccessReader openDataReader(RateLimiter limiter, FileAccessType accessType) {
      assert limiter != null;

      return this.dataFile.createReader(limiter, accessType);
   }

   public RandomAccessReader openDataReader() {
      return this.dataFile.createReader();
   }

   public RandomAccessReader openDataReader(FileAccessType accessType) {
      return this.dataFile.createReader(Rebufferer.ReaderConstraint.NONE, accessType);
   }

   public RandomAccessReader openDataReader(Rebufferer.ReaderConstraint rc, FileAccessType accessType) {
      return this.dataFile.createReader(rc, accessType);
   }

   public AsynchronousChannelProxy getDataChannel() {
      return this.dataFile.channel;
   }

   public long getCreationTimeFor(Component component) {
      return (new File(this.descriptor.filenameFor(component))).lastModified();
   }

   public long getKeyCacheHit() {
      return 0L;
   }

   public long getKeyCacheRequest() {
      return 0L;
   }

   public void incrementReadCount() {
      if(this.readMeter != null) {
         this.readMeter.mark();
      }

   }

   public EncodingStats stats() {
      return this.stats;
   }

   public Ref<SSTableReader> tryRef() {
      return this.selfRef.tryRef();
   }

   public Ref<SSTableReader> selfRef() {
      return this.selfRef;
   }

   public Ref<SSTableReader> ref() {
      return this.selfRef.ref();
   }

   public void runOnClose(AutoCloseable runOnClose) {
      synchronized(this.tidy.global) {
         this.tidy.addCloseable(runOnClose);
      }
   }

   protected void setup(boolean trackHotness) {
      this.tidy.setup(this, trackHotness);
      this.tidy.addCloseable(this.dataFile);
      this.tidy.addCloseable(this.bf);
   }

   @VisibleForTesting
   public void overrideReadMeter(RestorableMeter readMeter) {
      this.readMeter = this.tidy.global.readMeter = readMeter;
   }

   public void addTo(Ref.IdentityCollection identities) {
      identities.add((SelfRefCounted)this);
      identities.add(this.tidy.globalRef);
      this.dataFile.addTo(identities);
      this.bf.addTo(identities);
   }

   public void lock(MemoryOnlyStatus instance) {
      Throwable ret = Throwables.perform(null, Arrays.stream(this.getFilesToBeLocked()).map(f -> () -> f.lock(instance)));
      if (ret != null) {
         JVMStabilityInspector.inspectThrowable(ret);
         logger.error("Failed to lock {}", (Object)this, (Object)ret);
      }
   }

   public void unlock(MemoryOnlyStatus instance) {
      Throwable ret = Throwables.perform(null, Arrays.stream(this.getFilesToBeLocked()).map(f -> () -> f.unlock(instance)));
      if (ret != null) {
         JVMStabilityInspector.inspectThrowable(ret);
         logger.error("Failed to unlock {}", (Object)this, (Object)ret);
      }
   }

   public Iterable<MemoryLockedBuffer> getLockedMemory() {
      return Iterables.concat((Iterable)Arrays.stream(this.getFilesToBeLocked()).map((f) -> {
         return f.getLockedMemory();
      }).collect(Collectors.toList()));
   }

   protected abstract FileHandle[] getFilesToBeLocked();

   @VisibleForTesting
   public static void resetTidying() {
      SSTableReader.GlobalTidy.lookup.clear();
   }

   static {
      sstableOrdering = Ordering.from(sstableComparator);
      sizeComparator = new Comparator<SSTableReader>() {
         public int compare(SSTableReader o1, SSTableReader o2) {
            return Longs.compare(o1.onDiskLength(), o2.onDiskLength());
         }
      };
   }

   public abstract static class Factory {
      public Factory() {
      }

      public abstract SSTableReader open(Descriptor var1, Set<Component> var2, TableMetadataRef var3, Long var4, StatsMetadata var5, SSTableReader.OpenReason var6, SerializationHeader var7);

      public abstract Set<Component> requiredComponents();

      public abstract PartitionIndexIterator keyIterator(Descriptor var1, TableMetadata var2);

      public abstract Pair<DecoratedKey, DecoratedKey> getKeyRange(Descriptor var1, IPartitioner var2) throws IOException;
   }

   static final class GlobalTidy implements RefCounted.Tidy {
      static WeakReference<ScheduledFuture<?>> NULL = new WeakReference((Object)null);
      static final ConcurrentMap<Descriptor, Ref<SSTableReader.GlobalTidy>> lookup = new ConcurrentHashMap();
      private final Descriptor desc;
      private RestorableMeter readMeter;
      private WeakReference<ScheduledFuture<?>> readMeterSyncFuture;
      private volatile Runnable obsoletion;

      GlobalTidy(SSTableReader reader) {
         this.readMeterSyncFuture = NULL;
         this.desc = reader.descriptor;
      }

      CompletableFuture<Void> ensureReadMeter() {
         if(this.readMeter != null) {
            return TPCUtils.completedFuture(null);
         } else if(!SchemaConstants.isLocalSystemKeyspace(this.desc.ksname) && !DatabaseDescriptor.isClientOrToolInitialized()) {
            return SystemKeyspace.getSSTableReadMeter(this.desc.ksname, this.desc.cfname, this.desc.generation).thenAccept(this::setReadMeter);
         } else {
            this.readMeter = null;
            this.readMeterSyncFuture = NULL;
            return TPCUtils.completedFuture(null);
         }
      }

      private void setReadMeter(final RestorableMeter readMeter) {
         this.readMeter = readMeter;

         try {
            this.readMeterSyncFuture = new WeakReference(SSTableReader.readHotnessTrackerExecutor.scheduleAtFixedRate(new Runnable() {
               public void run() {
                  if(GlobalTidy.this.obsoletion == null) {
                     SSTableReader.meterSyncThrottle.acquire();
                     TPCUtils.blockingAwait(SystemKeyspace.persistSSTableReadMeter(GlobalTidy.this.desc.ksname, GlobalTidy.this.desc.cfname, GlobalTidy.this.desc.generation, readMeter));
                  }

               }
            }, 1L, 5L, TimeUnit.MINUTES));
         } catch (RejectedExecutionException var3) {
            ;
         }

      }

      private void stopReadMeterPersistence() {
         ScheduledFuture<?> readMeterSyncFutureLocal = (ScheduledFuture)this.readMeterSyncFuture.get();
         if(readMeterSyncFutureLocal != null) {
            readMeterSyncFutureLocal.cancel(true);
            this.readMeterSyncFuture = NULL;
         }

      }

      public void tidy() {
         lookup.remove(this.desc);
         if(this.obsoletion != null) {
            this.obsoletion.run();
         }

         NativeLibrary.trySkipCache(this.desc.filenameFor(Component.DATA), 0L, 0L);
         NativeLibrary.trySkipCache(this.desc.filenameFor(Component.ROW_INDEX), 0L, 0L);
         NativeLibrary.trySkipCache(this.desc.filenameFor(Component.PARTITION_INDEX), 0L, 0L);
      }

      public String name() {
         return this.desc.toString();
      }

      public static Ref<SSTableReader.GlobalTidy> get(SSTableReader sstable) {
         Descriptor descriptor = sstable.descriptor;
         Ref<SSTableReader.GlobalTidy> refc = (Ref)lookup.get(descriptor);
         if(refc != null) {
            return refc.ref();
         } else {
            SSTableReader.GlobalTidy tidy = new SSTableReader.GlobalTidy(sstable);
            refc = new Ref(tidy, tidy);
            Ref<?> ex = (Ref)lookup.putIfAbsent(descriptor, refc);
            if(ex != null) {
               refc.close();
               throw new AssertionError();
            } else {
               return refc;
            }
         }
      }
   }

   protected static final class InstanceTidier implements RefCounted.Tidy {
      private final Descriptor descriptor;
      private final TableId tableId;
      List<AutoCloseable> toClose;
      private boolean isReplaced = false;
      private Ref<SSTableReader.GlobalTidy> globalRef;
      private SSTableReader.GlobalTidy global;
      private volatile CompletableFuture<Void> setupFuture;

      void setup(SSTableReader reader, boolean trackHotness) {
         this.toClose = new ArrayList();
         this.globalRef = SSTableReader.GlobalTidy.get(reader);
         this.global = (SSTableReader.GlobalTidy)this.globalRef.get();
         this.setupFuture = this.ensureReadMeter(trackHotness).thenAccept((done) -> {
            reader.readMeter = this.global.readMeter;
         });
      }

      CompletableFuture<Void> ensureReadMeter(boolean trackHotness) {
         return trackHotness?this.global.ensureReadMeter():TPCUtils.completedFuture(null);
      }

      InstanceTidier(Descriptor descriptor, TableId tableId) {
         this.descriptor = descriptor;
         this.tableId = tableId;
      }

      public void addCloseable(AutoCloseable closeable) {
         if(closeable != null) {
            this.toClose.add(closeable);
         }

      }

      public void tidy() {
         if(SSTableReader.logger.isTraceEnabled()) {
            SSTableReader.logger.trace("Running instance tidier for {} with setup {}", this.descriptor, Boolean.valueOf(this.setupFuture != null));
         }

         if(this.setupFuture != null) {
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(this.tableId);
            final OpOrder.Barrier barrier;
            if(cfs != null) {
               barrier = cfs.readOrdering.newBarrier();
               barrier.issue();
            } else {
               barrier = null;
            }

            ScheduledExecutors.nonPeriodicTasks.execute(new Runnable() {
               public void run() {
                  if(SSTableReader.logger.isTraceEnabled()) {
                     SSTableReader.logger.trace("Async instance tidier for {}, before barrier", InstanceTidier.this.descriptor);
                  }

                  TPCUtils.blockingAwait(InstanceTidier.this.setupFuture);
                  if(barrier != null) {
                     barrier.await();
                  }

                  if(SSTableReader.logger.isTraceEnabled()) {
                     SSTableReader.logger.trace("Async instance tidier for {}, after barrier", InstanceTidier.this.descriptor);
                  }

                  Throwables.maybeFail(Throwables.close((Throwable)null, Lists.reverse(InstanceTidier.this.toClose)));
                  InstanceTidier.this.globalRef.release();
                  if(SSTableReader.logger.isTraceEnabled()) {
                     SSTableReader.logger.trace("Async instance tidier for {}, completed", InstanceTidier.this.descriptor);
                  }

               }
            });
         }
      }

      public String name() {
         return this.descriptor.toString();
      }
   }

   public static enum Operator {
      EQ {
         public int apply(int comparison) {
            return -comparison;
         }
      },
      GE {
         public int apply(int comparison) {
            return comparison >= 0?0:1;
         }
      },
      GT {
         public int apply(int comparison) {
            return comparison > 0?0:1;
         }
      };

      private Operator() {
      }

      public abstract int apply(int var1);
   }

   private final class KeysRange {
      PartitionPosition left;
      boolean inclusiveLeft;
      PartitionPosition right;
      boolean inclusiveRight;

      KeysRange(AbstractBounds<PartitionPosition> bounds) {
         assert !AbstractBounds.strictlyWrapsAround(bounds.left, bounds.right) : "[" + bounds.left + "," + bounds.right + "]";

         this.left = (PartitionPosition)bounds.left;
         this.inclusiveLeft = bounds.inclusiveLeft();
         if(SSTableReader.this.filterFirst() && SSTableReader.this.first.compareTo(this.left) > 0) {
            this.left = SSTableReader.this.first;
            this.inclusiveLeft = true;
         }

         this.right = (PartitionPosition)bounds.right;
         this.inclusiveRight = bounds.inclusiveRight();
         if(SSTableReader.this.filterLast() && SSTableReader.this.last.compareTo(this.right) < 0) {
            this.right = SSTableReader.this.last;
            this.inclusiveRight = true;
         }

      }

      PartitionIndexIterator iterator() throws IOException {
         return SSTableReader.this.coveredKeysIterator(this.left, this.inclusiveLeft, this.right, this.inclusiveRight);
      }

      public Flow<IndexFileEntry> flow(RandomAccessReader dataFileReader) {
         return SSTableReader.this.coveredKeysFlow(dataFileReader, this.left, this.inclusiveLeft, this.right, this.inclusiveRight);
      }
   }

   protected interface PartitionReader extends Closeable {
      Unfiltered next() throws IOException;

      void resetReaderState() throws IOException;
   }

   private static class DropPageCache implements Closeable {
      final FileHandle dfile;
      final long dfilePosition;
      final FileHandle ifile;
      final long ifilePosition;

      private DropPageCache(FileHandle dfile, long dfilePosition, FileHandle ifile, long ifilePosition) {
         this.dfile = dfile;
         this.dfilePosition = dfilePosition;
         this.ifile = ifile;
         this.ifilePosition = ifilePosition;
      }

      public void close() {
         this.dfile.dropPageCache(this.dfilePosition);
         if(this.ifile != null) {
            this.ifile.dropPageCache(this.ifilePosition);
         }

      }
   }

   public static enum OpenReason {
      NORMAL,
      EARLY,
      METADATA_CHANGE,
      MOVED_START;

      private OpenReason() {
      }
   }

   public static final class UniqueIdentifier {
      public UniqueIdentifier() {
      }
   }
}
