package org.apache.cassandra.io.sstable.format.big;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.cache.InstrumentingCache;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.IndexFileEntry;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.ScrubPartitionIterator;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.flow.Flow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigTableReader extends SSTableReader {
   private static final Logger logger = LoggerFactory.getLogger(BigTableReader.class);
   protected FileHandle ifile;
   protected IndexSummary indexSummary;
   public final BigRowIndexEntry.IndexSerializer rowIndexEntrySerializer;
   protected InstrumentingCache<KeyCacheKey, BigRowIndexEntry> keyCache;
   protected final AtomicLong keyCacheHit = new AtomicLong(0L);
   protected final AtomicLong keyCacheRequest = new AtomicLong(0L);

   BigTableReader(Descriptor desc, Set<Component> components, TableMetadataRef metadata, Long maxDataAge, StatsMetadata sstableMetadata, SSTableReader.OpenReason openReason, SerializationHeader header) {
      super(desc, components, metadata, maxDataAge.longValue(), sstableMetadata, openReason, header);
      this.rowIndexEntrySerializer = new BigRowIndexEntry.Serializer(this.descriptor.version, header);
   }

   protected void loadIndex(boolean preloadIfMemmapped) throws IOException {
      if(this.components.contains(Component.PRIMARY_INDEX)) {
         FileHandle.Builder ibuilder = (new FileHandle.Builder(this.descriptor.filenameFor(Component.PRIMARY_INDEX))).mmapped(this.metadata().indexAccessMode == Config.AccessMode.mmap).withChunkCache(ChunkCache.instance);
         Throwable var3 = null;

         try {
            this.loadSummary();
            long indexFileLength = (new File(this.descriptor.filenameFor(Component.PRIMARY_INDEX))).length();
            int indexBufferSize = this.optimizationStrategy.bufferSize(indexFileLength / (long)this.indexSummary.size());
            this.ifile = ibuilder.bufferSize(indexBufferSize).complete();
         } catch (Throwable var14) {
            var3 = var14;
            throw var14;
         } finally {
            if(ibuilder != null) {
               if(var3 != null) {
                  try {
                     ibuilder.close();
                  } catch (Throwable var13) {
                     var3.addSuppressed(var13);
                  }
               } else {
                  ibuilder.close();
               }
            }

         }

      }
   }

   public boolean loadSummary() {
      File summariesFile = new File(this.descriptor.filenameFor(Component.SUMMARY));
      if(!summariesFile.exists()) {
         return false;
      } else {
         DataInputStream iStream = null;

         boolean var4;
         try {
            iStream = new DataInputStream(Files.newInputStream(summariesFile.toPath(), new OpenOption[0]));
            this.indexSummary = IndexSummary.serializer.deserialize(iStream, this.getPartitioner(), this.metadata().params.minIndexInterval, this.metadata().params.maxIndexInterval);
            this.first = this.decorateKey(ByteBufferUtil.readWithLength(iStream));
            this.last = this.decorateKey(ByteBufferUtil.readWithLength(iStream));
            return true;
         } catch (IOException var8) {
            if(this.indexSummary != null) {
               this.indexSummary.close();
            }

            logger.trace("Cannot deserialize SSTable Summary File {}: {}", summariesFile.getPath(), var8.getMessage());
            FileUtils.closeQuietly((Closeable)iStream);
            FileUtils.deleteWithConfirm(summariesFile);
            var4 = false;
         } finally {
            FileUtils.closeQuietly((Closeable)iStream);
         }

         return var4;
      }
   }

   protected void releaseIndex() {
      if(this.ifile != null) {
         this.ifile.close();
         this.ifile = null;
      }

      if(this.indexSummary != null) {
         this.indexSummary.close();
         this.indexSummary = null;
      }

   }

   protected SSTableReader clone(SSTableReader.OpenReason reason) {
      BigTableReader replacement = internalOpen(this.descriptor, this.components, this.metadata, this.ifile.sharedCopy(), this.dataFile.sharedCopy(), this.indexSummary.sharedCopy(), this.bf.sharedCopy(), this.maxDataAge, this.sstableMetadata, reason, this.header);
      replacement.first = this.first;
      replacement.last = this.last;
      replacement.isSuspect.set(this.isSuspect.get());
      return replacement;
   }

   static BigTableReader internalOpen(Descriptor desc, Set<Component> components, TableMetadataRef metadata, FileHandle ifile, FileHandle dfile, IndexSummary indexSummary, IFilter bf, long maxDataAge, StatsMetadata sstableMetadata, SSTableReader.OpenReason openReason, SerializationHeader header) {
      assert desc != null && ifile != null && dfile != null && indexSummary != null && bf != null && sstableMetadata != null;

      assert desc.getFormat() instanceof BigFormat;

      BigTableReader reader = BigFormat.readerFactory.open(desc, components, metadata, Long.valueOf(maxDataAge), sstableMetadata, openReason, header);
      reader.bf = bf;
      reader.ifile = ifile;
      reader.dataFile = dfile;
      reader.indexSummary = indexSummary;
      reader.setup(true);
      return reader;
   }

   protected void setup(boolean trackHotness) {
      super.setup(trackHotness);
      this.tidy.addCloseable(this.ifile);
      this.tidy.addCloseable(this.indexSummary);
   }

   public void setupOnline() {
      super.setupOnline();
      this.keyCache = CacheService.instance.keyCache;
      logger.trace("key cache contains {}/{} keys", Integer.valueOf(this.keyCache.size()), Long.valueOf(this.keyCache.getCapacity()));
   }

   public boolean isKeyCacheSetup() {
      return this.keyCache != null;
   }

   public void addTo(Ref.IdentityCollection identities) {
      super.addTo(identities);
      this.ifile.addTo(identities);
      this.indexSummary.addTo(identities);
   }

   public SSTableReader.PartitionReader reader(FileDataInput file, boolean shouldCloseFile, RowIndexEntry indexEntry, SerializationHelper helper, Slices slices, boolean reversed, Rebufferer.ReaderConstraint readerConstraint) throws IOException {
      return (SSTableReader.PartitionReader)(indexEntry.isIndexed()?(reversed?new ReverseIndexedReader(this, (BigRowIndexEntry)indexEntry, slices, file, shouldCloseFile, helper, readerConstraint):new ForwardIndexedReader(this, (BigRowIndexEntry)indexEntry, slices, file, shouldCloseFile, helper, readerConstraint)):(reversed?new ReverseReader(this, slices, file, shouldCloseFile, helper):new ForwardReader(this, slices, file, shouldCloseFile, helper)));
   }

   public long getIndexScanPosition(PartitionPosition key) {
      if(this.openReason == SSTableReader.OpenReason.MOVED_START && ((PartitionPosition)key).compareTo(this.first) < 0) {
         key = this.first;
      }

      return getIndexScanPositionFromBinarySearchResult(this.indexSummary.binarySearch((PartitionPosition)key), this.indexSummary);
   }

   public static long getIndexScanPositionFromBinarySearchResult(int binarySearchResult, IndexSummary referencedIndexSummary) {
      return binarySearchResult == -1?0L:referencedIndexSummary.getPosition(getIndexSummaryIndexFromBinarySearchResult(binarySearchResult));
   }

   public static int getIndexSummaryIndexFromBinarySearchResult(int binarySearchResult) {
      if(binarySearchResult < 0) {
         int greaterThan = (binarySearchResult + 1) * -1;
         return greaterThan == 0?-1:greaterThan - 1;
      } else {
         return binarySearchResult;
      }
   }

   public DecoratedKey keyAt(long indexPosition, Rebufferer.ReaderConstraint rc) throws IOException {
      FileDataInput in = this.ifile.createReader(indexPosition, rc);
      Throwable var6 = null;

      Object var7;
      try {
         if(!in.isEOF()) {
            DecoratedKey key = this.decorateKey(ByteBufferUtil.readWithShortLength(in));
            if(this.isKeyCacheSetup()) {
               this.cacheKey(key, this.rowIndexEntrySerializer.deserialize(in, in.getFilePointer()));
            }

            return key;
         }

         var7 = null;
      } catch (Throwable var17) {
         var6 = var17;
         throw var17;
      } finally {
         if(in != null) {
            if(var6 != null) {
               try {
                  in.close();
               } catch (Throwable var16) {
                  var6.addSuppressed(var16);
               }
            } else {
               in.close();
            }
         }

      }

      return (DecoratedKey)var7;
   }

   public KeyCacheKey getCacheKey(DecoratedKey key) {
      return new KeyCacheKey(this.metadata(), this.descriptor, key.getKey());
   }

   public void cacheKey(DecoratedKey key, BigRowIndexEntry info) {
      CachingParams caching = this.metadata().params.caching;
      if(caching.cacheKeys() && this.keyCache != null && this.keyCache.getCapacity() != 0L) {
         KeyCacheKey cacheKey = new KeyCacheKey(this.metadata(), this.descriptor, key.getKey());
         logger.trace("Adding cache entry for {} -> {}", cacheKey, info);
         this.keyCache.put(cacheKey, info);
      }
   }

   public BigRowIndexEntry getCachedPosition(DecoratedKey key, boolean updateStats) {
      return this.getCachedPosition(new KeyCacheKey(this.metadata(), this.descriptor, key.getKey()), updateStats);
   }

   public BigRowIndexEntry getCachedPosition(KeyCacheKey unifiedKey, boolean updateStats) {
      if(this.keyCacheEnabled()) {
         if(updateStats) {
            BigRowIndexEntry cachedEntry = (BigRowIndexEntry)this.keyCache.get(unifiedKey);
            this.keyCacheRequest.incrementAndGet();
            if(cachedEntry != null) {
               this.keyCacheHit.incrementAndGet();
               this.bloomFilterTracker.addTruePositive();
            }

            return cachedEntry;
         } else {
            return (BigRowIndexEntry)this.keyCache.getInternal(unifiedKey);
         }
      } else {
         return null;
      }
   }

   private boolean keyCacheEnabled() {
      return this.keyCache != null && this.keyCache.getCapacity() > 0L && this.metadata().params.caching.cacheKeys();
   }

   public InstrumentingCache<KeyCacheKey, BigRowIndexEntry> getKeyCache() {
      return this.keyCache;
   }

   public long getKeyCacheHit() {
      return this.keyCacheHit.get();
   }

   public long getKeyCacheRequest() {
      return this.keyCacheRequest.get();
   }

   public BigRowIndexEntry getPosition(PartitionPosition key, SSTableReader.Operator op, Rebufferer.ReaderConstraint rc) {
      return this.getPosition(key, op, true, false, SSTableReadsListener.NOOP_LISTENER, rc);
   }

   public BigRowIndexEntry getPosition(PartitionPosition key, SSTableReader.Operator op, SSTableReadsListener listener, Rebufferer.ReaderConstraint rc) {
      return this.getPosition(key, op, true, false, listener, rc);
   }

   protected BigRowIndexEntry getPosition(PartitionPosition key, SSTableReader.Operator op, boolean updateCacheAndStats, boolean permitMatchPastLast, SSTableReadsListener listener, Rebufferer.ReaderConstraint rc) {
      if (op == SSTableReader.Operator.EQ) {
         assert (key instanceof DecoratedKey);
         if (!this.bf.isPresent((DecoratedKey)key)) {
            listener.onSSTableSkipped(this, SSTableReadsListener.SkippingReason.BLOOM_FILTER);
            Tracing.trace("Bloom filter allows skipping sstable {}", (Object)this.descriptor.generation);
            return null;
         }
      }
      if ((op == SSTableReader.Operator.EQ || op == SSTableReader.Operator.GE) && key instanceof DecoratedKey) {
         DecoratedKey decoratedKey = (DecoratedKey)key;
         KeyCacheKey cacheKey = new KeyCacheKey(this.metadata(), this.descriptor, decoratedKey.getKey());
         BigRowIndexEntry cachedPosition = this.getCachedPosition(cacheKey, updateCacheAndStats);
         if (cachedPosition != null) {
            listener.onSSTableSelected(this, cachedPosition, SSTableReadsListener.SelectionReason.KEY_CACHE_HIT);
            Tracing.trace("Key cache hit for sstable {}", (Object)this.descriptor.generation);
            return cachedPosition;
         }
      }
      boolean skip = false;
      if (key.compareTo(this.first) < 0) {
         if (op == SSTableReader.Operator.EQ) {
            skip = true;
         } else {
            key = this.first;
         }
         op = SSTableReader.Operator.EQ;
      } else {
         int l = this.last.compareTo(key);
         boolean bl = skip = l <= 0 && (l < 0 || !permitMatchPastLast && op == SSTableReader.Operator.GT);
      }
      if (skip) {
         if (op == SSTableReader.Operator.EQ && updateCacheAndStats) {
            this.bloomFilterTracker.addFalsePositive();
         }
         listener.onSSTableSkipped(this, SSTableReadsListener.SkippingReason.MIN_MAX_KEYS);
         Tracing.trace("Check against min and max keys allows skipping sstable {}", (Object)this.descriptor.generation);
         return null;
      }
      int binarySearchResult = this.indexSummary.binarySearch(key);
      long sampledPosition = BigTableReader.getIndexScanPositionFromBinarySearchResult(binarySearchResult, this.indexSummary);
      int sampledIndex = BigTableReader.getIndexSummaryIndexFromBinarySearchResult(binarySearchResult);
      int effectiveInterval = this.indexSummary.getEffectiveIndexIntervalAfterIndex(sampledIndex);
      if (this.ifile == null) {
         return null;
      }
      int i = 0;
      String path = null;
      try {
         try (FileDataInput in = this.ifile.createReader(sampledPosition, rc);){
            path = in.getPath();
            while (!in.isEOF()) {
               boolean opSatisfied;
               boolean exactMatch;
               ByteBuffer indexKey = ByteBufferUtil.readWithShortLength(in);
               if (op == SSTableReader.Operator.EQ && ++i <= effectiveInterval) {
                  opSatisfied = exactMatch = indexKey.equals(((DecoratedKey)key).getKey());
               } else {
                  DecoratedKey indexDecoratedKey = this.decorateKey(indexKey);
                  int comparison = indexDecoratedKey.compareTo(key);
                  int v = op.apply(comparison);
                  opSatisfied = v == 0;
                  boolean bl = exactMatch = comparison == 0;
                  if (v < 0) {
                     listener.onSSTableSkipped(this, SSTableReadsListener.SkippingReason.PARTITION_INDEX_LOOKUP);
                     Tracing.trace("Partition index lookup allows skipping sstable {}", (Object)this.descriptor.generation);
                     BigRowIndexEntry bigRowIndexEntry = null;
                     return bigRowIndexEntry;
                  }
               }
               if (opSatisfied) {
                  BigRowIndexEntry indexEntry = this.rowIndexEntrySerializer.deserialize(in, in.getFilePointer());
                  if (exactMatch && updateCacheAndStats) {
                     DecoratedKey decoratedKey;
                     assert (key instanceof DecoratedKey);
                     decoratedKey = (DecoratedKey)key;
                     if (logger.isTraceEnabled()) {
                        try (FileDataInput fdi = this.dataFile.createReader(indexEntry.position, rc);){
                           DecoratedKey keyInDisk = this.decorateKey(ByteBufferUtil.readWithShortLength(fdi));
                           if (!keyInDisk.equals(key)) {
                              throw new AssertionError((Object)String.format("%s != %s in %s", keyInDisk, key, fdi.getPath()));
                           }
                        }
                     }
                     this.cacheKey(decoratedKey, indexEntry);
                  }
                  if (op == SSTableReader.Operator.EQ && updateCacheAndStats) {
                     this.bloomFilterTracker.addTruePositive();
                  }
                  listener.onSSTableSelected(this, indexEntry, SSTableReadsListener.SelectionReason.INDEX_ENTRY_FOUND);
                  Tracing.trace("Partition index with {} entries found for sstable {}", (Object)indexEntry.rowIndexCount(), (Object)this.descriptor.generation);
                  BigRowIndexEntry decoratedKey = indexEntry;
                  return decoratedKey;
               }
               BigRowIndexEntry.Serializer.skip(in, this.descriptor.version);
            }
         }
      }
      catch (IOException e) {
         this.markSuspect();
         throw new CorruptSSTableException((Throwable)e, path);
      }
      if (op == SSTableReader.Operator.EQ && updateCacheAndStats) {
         this.bloomFilterTracker.addFalsePositive();
      }
      listener.onSSTableSkipped(this, SSTableReadsListener.SkippingReason.INDEX_ENTRY_NOT_FOUND);
      Tracing.trace("Partition index lookup complete (bloom filter false positive) for sstable {}", (Object)this.descriptor.generation);
      return null;
   }

   public long estimatedKeys() {
      return this.indexSummary.getEstimatedKeyCount();
   }

   public long estimatedKeysForRanges(Collection<Range<Token>> ranges) {
      long sampleKeyCount = 0L;
      List<Pair<Integer, Integer>> sampleIndexes = getSampleIndexesForRanges(this.indexSummary, ranges);

      Pair sampleIndexRange;
      for(Iterator var5 = sampleIndexes.iterator(); var5.hasNext(); sampleKeyCount += (long)(((Integer)sampleIndexRange.right).intValue() - ((Integer)sampleIndexRange.left).intValue() + 1)) {
         sampleIndexRange = (Pair)var5.next();
      }

      long estimatedKeys = sampleKeyCount * 128L * (long)this.indexSummary.getMinIndexInterval() / (long)this.indexSummary.getSamplingLevel();
      return Math.max(1L, estimatedKeys);
   }

   private static List<Pair<Integer, Integer>> getSampleIndexesForRanges(IndexSummary summary, Collection<Range<Token>> ranges) {
      List<Pair<Integer, Integer>> positions = new ArrayList();
      Iterator var3 = Range.normalize(ranges).iterator();

      while(true) {
         int left;
         int right;
         while(true) {
            Range range;
            Token.KeyBound rightPosition;
            do {
               if(!var3.hasNext()) {
                  return positions;
               }

               range = (Range)var3.next();
               PartitionPosition leftPosition = ((Token)range.left).maxKeyBound();
               rightPosition = ((Token)range.right).maxKeyBound();
               left = summary.binarySearch(leftPosition);
               if(left < 0) {
                  left = (left + 1) * -1;
               } else {
                  ++left;
               }
            } while(left == summary.size());

            right = range.isWrapAround()?summary.size() - 1:summary.binarySearch(rightPosition);
            if(right >= 0) {
               break;
            }

            right = (right + 1) * -1;
            if(right != 0) {
               --right;
               break;
            }
         }

         if(left <= right) {
            positions.add(Pair.create(Integer.valueOf(left), Integer.valueOf(right)));
         }
      }
   }

   public Iterable<DecoratedKey> getKeySamples(Range<Token> range) {
      final List<Pair<Integer, Integer>> indexRanges = getSampleIndexesForRanges(this.indexSummary, Collections.singletonList(range));
      return (Iterable)(indexRanges.isEmpty()?Collections.emptyList():new Iterable<DecoratedKey>() {
         public Iterator<DecoratedKey> iterator() {
            return new Iterator<DecoratedKey>() {
               private Iterator<Pair<Integer, Integer>> rangeIter = indexRanges.iterator();
               private Pair<Integer, Integer> current;
               private int idx;

               public boolean hasNext() {
                  if(this.current != null && this.idx <= ((Integer)this.current.right).intValue()) {
                     return true;
                  } else if(this.rangeIter.hasNext()) {
                     this.current = (Pair)this.rangeIter.next();
                     this.idx = ((Integer)this.current.left).intValue();
                     return true;
                  } else {
                     return false;
                  }
               }

               public DecoratedKey next() {
                  byte[] bytes = BigTableReader.this.indexSummary.getKey(this.idx++);
                  return BigTableReader.this.decorateKey(ByteBuffer.wrap(bytes));
               }

               public void remove() {
                  throw new UnsupportedOperationException();
               }
            };
         }
      });
   }

   public RowIndexEntry getExactPosition(DecoratedKey key, SSTableReadsListener listener, Rebufferer.ReaderConstraint rc) {
      return this.getPosition(key, SSTableReader.Operator.EQ, listener, rc);
   }

   public boolean contains(DecoratedKey key, Rebufferer.ReaderConstraint rc) {
      return this.getExactPosition(key, SSTableReadsListener.NOOP_LISTENER, rc) != null;
   }

   public PartitionIterator coveredKeysIterator(PartitionPosition left, boolean inclusiveLeft, PartitionPosition right, boolean inclusiveRight) throws IOException {
      return new PartitionIterator(this, left, inclusiveLeft?-1:0, right, inclusiveRight?0:-1);
   }

   public PartitionIndexIterator allKeysIterator() throws IOException {
      return new PartitionIterator(this);
   }

   public ScrubPartitionIterator scrubPartitionsIterator() throws IOException {
      return this.ifile == null?null:new ScrubIterator(this.ifile, this.rowIndexEntrySerializer);
   }

   public Flow<IndexFileEntry> coveredKeysFlow(RandomAccessReader dfile, PartitionPosition left, boolean inclusiveLeft, PartitionPosition right, boolean inclusiveRight) {
      return new BigIndexFileFlow(this, left, inclusiveLeft?-1:0, right, inclusiveRight?0:-1);
   }

   protected FileHandle[] getFilesToBeLocked() {
      return new FileHandle[]{this.dataFile, this.ifile};
   }
}
