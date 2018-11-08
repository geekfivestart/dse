package org.apache.cassandra.service;

import com.google.common.util.concurrent.Futures;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.cache.CacheProvider;
import org.apache.cassandra.cache.CaffeineCache;
import org.apache.cassandra.cache.CounterCacheKey;
import org.apache.cassandra.cache.ICache;
import org.apache.cassandra.cache.IRowCacheEntry;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.cache.RowCacheKey;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClockAndCount;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.partitions.CachedBTreePartition;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigRowIndexEntry;
import org.apache.cassandra.io.sstable.format.big.BigTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.flow.Flow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheService implements CacheServiceMBean {
   private static final Logger logger = LoggerFactory.getLogger(CacheService.class);
   public static final String MBEAN_NAME = "org.apache.cassandra.db:type=Caches";
   public static final CacheService instance = new CacheService();
   public final AutoSavingCache<KeyCacheKey, BigRowIndexEntry> keyCache;
   public final AutoSavingCache<RowCacheKey, IRowCacheEntry> rowCache;
   public final AutoSavingCache<CounterCacheKey, ClockAndCount> counterCache;

   private CacheService() {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

      try {
         mbs.registerMBean(this, new ObjectName("org.apache.cassandra.db:type=Caches"));
      } catch (Exception var3) {
         throw new RuntimeException(var3);
      }

      this.keyCache = this.initKeyCache();
      this.rowCache = this.initRowCache();
      this.counterCache = this.initCounterCache();
   }

   private AutoSavingCache<KeyCacheKey, BigRowIndexEntry> initKeyCache() {
      logger.info("Initializing key cache with capacity of {} MBs.", Long.valueOf(DatabaseDescriptor.getKeyCacheSizeInMB()));
      long keyCacheInMemoryCapacity = DatabaseDescriptor.getKeyCacheSizeInMB() * 1024L * 1024L;
      ICache<KeyCacheKey, BigRowIndexEntry> kc = CaffeineCache.create(keyCacheInMemoryCapacity);
      AutoSavingCache<KeyCacheKey, BigRowIndexEntry> keyCache = new AutoSavingCache(kc, CacheService.CacheType.KEY_CACHE, new CacheService.KeyCacheSerializer());
      int keyCacheKeysToSave = DatabaseDescriptor.getKeyCacheKeysToSave();
      keyCache.scheduleSaving(DatabaseDescriptor.getKeyCacheSavePeriod(), keyCacheKeysToSave);
      return keyCache;
   }

   private AutoSavingCache<RowCacheKey, IRowCacheEntry> initRowCache() {
      logger.info("Initializing row cache with capacity of {} MBs", Long.valueOf(DatabaseDescriptor.getRowCacheSizeInMB()));
      String cacheProviderClassName = DatabaseDescriptor.getRowCacheSizeInMB() > 0L?DatabaseDescriptor.getRowCacheClassName():"org.apache.cassandra.cache.NopCacheProvider";

      CacheProvider cacheProvider;
      try {
         Class<CacheProvider<RowCacheKey, IRowCacheEntry>> cacheProviderClass = Class.forName(cacheProviderClassName);
         cacheProvider = (CacheProvider)cacheProviderClass.newInstance();
      } catch (Exception var6) {
         throw new RuntimeException("Cannot find configured row cache provider class " + DatabaseDescriptor.getRowCacheClassName());
      }

      ICache<RowCacheKey, IRowCacheEntry> rc = cacheProvider.create();
      AutoSavingCache<RowCacheKey, IRowCacheEntry> rowCache = new AutoSavingCache(rc, CacheService.CacheType.ROW_CACHE, new CacheService.RowCacheSerializer());
      int rowCacheKeysToSave = DatabaseDescriptor.getRowCacheKeysToSave();
      rowCache.scheduleSaving(DatabaseDescriptor.getRowCacheSavePeriod(), rowCacheKeysToSave);
      return rowCache;
   }

   private AutoSavingCache<CounterCacheKey, ClockAndCount> initCounterCache() {
      logger.info("Initializing counter cache with capacity of {} MBs", Long.valueOf(DatabaseDescriptor.getCounterCacheSizeInMB()));
      long capacity = DatabaseDescriptor.getCounterCacheSizeInMB() * 1024L * 1024L;
      AutoSavingCache<CounterCacheKey, ClockAndCount> cache = new AutoSavingCache(CaffeineCache.create(capacity), CacheService.CacheType.COUNTER_CACHE, new CacheService.CounterCacheSerializer());
      int keysToSave = DatabaseDescriptor.getCounterCacheKeysToSave();
      logger.info("Scheduling counter cache save to every {} seconds (going to save {} keys).", Integer.valueOf(DatabaseDescriptor.getCounterCacheSavePeriod()), keysToSave == 2147483647?"all":Integer.valueOf(keysToSave));
      cache.scheduleSaving(DatabaseDescriptor.getCounterCacheSavePeriod(), keysToSave);
      return cache;
   }

   public int getRowCacheSavePeriodInSeconds() {
      return DatabaseDescriptor.getRowCacheSavePeriod();
   }

   public void setRowCacheSavePeriodInSeconds(int seconds) {
      if(seconds < 0) {
         throw new RuntimeException("RowCacheSavePeriodInSeconds must be non-negative.");
      } else {
         DatabaseDescriptor.setRowCacheSavePeriod(seconds);
         this.rowCache.scheduleSaving(seconds, DatabaseDescriptor.getRowCacheKeysToSave());
      }
   }

   public int getKeyCacheSavePeriodInSeconds() {
      return DatabaseDescriptor.getKeyCacheSavePeriod();
   }

   public void setKeyCacheSavePeriodInSeconds(int seconds) {
      if(seconds < 0) {
         throw new RuntimeException("KeyCacheSavePeriodInSeconds must be non-negative.");
      } else {
         DatabaseDescriptor.setKeyCacheSavePeriod(seconds);
         this.keyCache.scheduleSaving(seconds, DatabaseDescriptor.getKeyCacheKeysToSave());
      }
   }

   public int getCounterCacheSavePeriodInSeconds() {
      return DatabaseDescriptor.getCounterCacheSavePeriod();
   }

   public void setCounterCacheSavePeriodInSeconds(int seconds) {
      if(seconds < 0) {
         throw new RuntimeException("CounterCacheSavePeriodInSeconds must be non-negative.");
      } else {
         DatabaseDescriptor.setCounterCacheSavePeriod(seconds);
         this.counterCache.scheduleSaving(seconds, DatabaseDescriptor.getCounterCacheKeysToSave());
      }
   }

   public int getRowCacheKeysToSave() {
      return DatabaseDescriptor.getRowCacheKeysToSave();
   }

   public void setRowCacheKeysToSave(int count) {
      if(count < 0) {
         throw new RuntimeException("RowCacheKeysToSave must be non-negative.");
      } else {
         DatabaseDescriptor.setRowCacheKeysToSave(count);
         this.rowCache.scheduleSaving(this.getRowCacheSavePeriodInSeconds(), count);
      }
   }

   public int getKeyCacheKeysToSave() {
      return DatabaseDescriptor.getKeyCacheKeysToSave();
   }

   public void setKeyCacheKeysToSave(int count) {
      if(count < 0) {
         throw new RuntimeException("KeyCacheKeysToSave must be non-negative.");
      } else {
         DatabaseDescriptor.setKeyCacheKeysToSave(count);
         this.keyCache.scheduleSaving(this.getKeyCacheSavePeriodInSeconds(), count);
      }
   }

   public int getCounterCacheKeysToSave() {
      return DatabaseDescriptor.getCounterCacheKeysToSave();
   }

   public void setCounterCacheKeysToSave(int count) {
      if(count < 0) {
         throw new RuntimeException("CounterCacheKeysToSave must be non-negative.");
      } else {
         DatabaseDescriptor.setCounterCacheKeysToSave(count);
         this.counterCache.scheduleSaving(this.getCounterCacheSavePeriodInSeconds(), count);
      }
   }

   public void invalidateKeyCache() {
      this.keyCache.clear();
   }

   public void invalidateKeyCacheForCf(TableMetadata tableMetadata) {
      Iterator keyCacheIterator = this.keyCache.keyIterator();

      while(keyCacheIterator.hasNext()) {
         KeyCacheKey key = (KeyCacheKey)keyCacheIterator.next();
         if(key.sameTable(tableMetadata)) {
            keyCacheIterator.remove();
         }
      }

   }

   public void invalidateRowCache() {
      this.rowCache.clear();
   }

   public void invalidateRowCacheForCf(TableMetadata tableMetadata) {
      Iterator rowCacheIterator = this.rowCache.keyIterator();

      while(rowCacheIterator.hasNext()) {
         RowCacheKey key = (RowCacheKey)rowCacheIterator.next();
         if(key.sameTable(tableMetadata)) {
            rowCacheIterator.remove();
         }
      }

   }

   public void invalidateCounterCacheForCf(TableMetadata tableMetadata) {
      Iterator counterCacheIterator = this.counterCache.keyIterator();

      while(counterCacheIterator.hasNext()) {
         CounterCacheKey key = (CounterCacheKey)counterCacheIterator.next();
         if(key.sameTable(tableMetadata)) {
            counterCacheIterator.remove();
         }
      }

   }

   public void invalidateCounterCache() {
      this.counterCache.clear();
   }

   public void setRowCacheCapacityInMB(long capacity) {
      if(capacity < 0L) {
         throw new RuntimeException("capacity should not be negative.");
      } else {
         this.rowCache.setCapacity(capacity * 1024L * 1024L);
      }
   }

   public void setKeyCacheCapacityInMB(long capacity) {
      if(capacity < 0L) {
         throw new RuntimeException("capacity should not be negative.");
      } else {
         this.keyCache.setCapacity(capacity * 1024L * 1024L);
      }
   }

   public void setCounterCacheCapacityInMB(long capacity) {
      if(capacity < 0L) {
         throw new RuntimeException("capacity should not be negative.");
      } else {
         this.counterCache.setCapacity(capacity * 1024L * 1024L);
      }
   }

   public void saveCaches() throws ExecutionException, InterruptedException {
      List<Future<?>> futures = new ArrayList(3);
      logger.debug("submitting cache saves");
      futures.add(this.keyCache.submitWrite(DatabaseDescriptor.getKeyCacheKeysToSave()));
      futures.add(this.rowCache.submitWrite(DatabaseDescriptor.getRowCacheKeysToSave()));
      futures.add(this.counterCache.submitWrite(DatabaseDescriptor.getCounterCacheKeysToSave()));
      FBUtilities.waitOnFutures(futures);
      logger.debug("cache saves completed");
   }

   public static class KeyCacheSerializer implements AutoSavingCache.CacheSerializer<KeyCacheKey, BigRowIndexEntry> {
      public KeyCacheSerializer() {
      }

      public void serialize(KeyCacheKey key, DataOutputPlus out, ColumnFamilyStore cfs) throws IOException {
         BigRowIndexEntry entry = (BigRowIndexEntry)CacheService.instance.keyCache.getInternal(key);
         if(entry != null) {
            TableMetadata tableMetadata = cfs.metadata();
            tableMetadata.id.serialize(out);
            out.writeUTF((String)tableMetadata.indexName().orElse(""));
            ByteBufferUtil.writeWithLength((byte[])key.key(), (DataOutput)out);
            out.writeInt(key.desc.generation);
            out.writeBoolean(true);
            SerializationHeader header = new SerializationHeader(false, cfs.metadata(), cfs.metadata().regularAndStaticColumns(), EncodingStats.NO_STATS);
            (new BigRowIndexEntry.Serializer(key.desc.version, header)).serializeForCache(entry, out);
         }
      }

      public Future<Pair<KeyCacheKey, BigRowIndexEntry>> deserialize(DataInputPlus input, ColumnFamilyStore cfs) throws IOException {
         int keyLength = input.readInt();
         if(keyLength > '\uffff') {
            throw new IOException(String.format("Corrupted key cache. Key length of %d is longer than maximum of %d", new Object[]{Integer.valueOf(keyLength), Integer.valueOf('\uffff')}));
         } else {
            ByteBuffer key = ByteBufferUtil.read(input, keyLength);
            int generation = input.readInt();
            input.readBoolean();
            BigTableReader reader;
            if(cfs != null && cfs.isKeyCacheEnabled() && (reader = this.findDesc(generation, cfs.getSSTables(SSTableSet.CANONICAL))) != null) {
               BigRowIndexEntry.IndexSerializer indexSerializer = reader.rowIndexEntrySerializer;
               BigRowIndexEntry entry = indexSerializer.deserializeForCache(input);
               return Futures.immediateFuture(Pair.create(new KeyCacheKey(cfs.metadata(), reader.descriptor, key), entry));
            } else {
               BigRowIndexEntry.Serializer.skipForCache(input);
               return null;
            }
         }
      }

      private BigTableReader findDesc(int generation, Iterable<SSTableReader> collection) {
         Iterator var3 = collection.iterator();

         SSTableReader sstable;
         do {
            if(!var3.hasNext()) {
               return null;
            }

            sstable = (SSTableReader)var3.next();
         } while(sstable.descriptor.formatType != SSTableFormat.Type.BIG || sstable.descriptor.generation != generation);

         return (BigTableReader)sstable;
      }
   }

   public static class RowCacheSerializer implements AutoSavingCache.CacheSerializer<RowCacheKey, IRowCacheEntry> {
      public RowCacheSerializer() {
      }

      public void serialize(RowCacheKey key, DataOutputPlus out, ColumnFamilyStore cfs) throws IOException {
         assert !cfs.isIndex();

         TableMetadata tableMetadata = cfs.metadata();
         tableMetadata.id.serialize(out);
         out.writeUTF((String)tableMetadata.indexName().orElse(""));
         ByteBufferUtil.writeWithLength((byte[])key.key, (DataOutput)out);
      }

      public Future<Pair<RowCacheKey, IRowCacheEntry>> deserialize(DataInputPlus in, ColumnFamilyStore cfs) throws IOException {
         ByteBuffer buffer = ByteBufferUtil.readWithLength(in);
         if(cfs != null && cfs.isRowCacheEnabled()) {
            int rowsToCache = cfs.metadata().params.caching.rowsPerPartitionToCache();

            assert !cfs.isIndex();

            return StageManager.getStage(Stage.BACKGROUND_IO).submit(() -> {
               DecoratedKey key = cfs.decorateKey(buffer);
               int nowInSec = FBUtilities.nowInSeconds();
               SinglePartitionReadCommand cmd = SinglePartitionReadCommand.fullPartitionRead(cfs.metadata(), nowInSec, key);
               ReadExecutionController controller = cmd.executionController();
               Throwable var7 = null;

               Pair var12;
               try {
                  Flow<FlowableUnfilteredPartition> flow = cmd.deferredQuery(cfs, controller);
                  UnfilteredRowIterator iter = FlowablePartitions.toIterator(DataLimits.cqlLimits(rowsToCache).truncateUnfiltered((FlowableUnfilteredPartition)flow.blockingSingle(), nowInSec, true, cmd.metadata().enforceStrictLiveness()));
                  Throwable var10 = null;

                  try {
                     CachedPartition toCache = CachedBTreePartition.create(iter, nowInSec);
                     var12 = Pair.create(new RowCacheKey(cfs.metadata(), key), toCache);
                  } catch (Throwable var35) {
                     var10 = var35;
                     throw var35;
                  } finally {
                     if(iter != null) {
                        if(var10 != null) {
                           try {
                              iter.close();
                           } catch (Throwable var34) {
                              var10.addSuppressed(var34);
                           }
                        } else {
                           iter.close();
                        }
                     }

                  }
               } catch (Throwable var37) {
                  var7 = var37;
                  throw var37;
               } finally {
                  if(controller != null) {
                     if(var7 != null) {
                        try {
                           controller.close();
                        } catch (Throwable var33) {
                           var7.addSuppressed(var33);
                        }
                     } else {
                        controller.close();
                     }
                  }

               }

               return var12;
            });
         } else {
            return null;
         }
      }
   }

   public static class CounterCacheSerializer implements AutoSavingCache.CacheSerializer<CounterCacheKey, ClockAndCount> {
      public CounterCacheSerializer() {
      }

      public void serialize(CounterCacheKey key, DataOutputPlus out, ColumnFamilyStore cfs) throws IOException {
         assert cfs.metadata().isCounter();

         TableMetadata tableMetadata = cfs.metadata();
         tableMetadata.id.serialize(out);
         out.writeUTF((String)tableMetadata.indexName().orElse(""));
         key.write(out);
      }

      public Future<Pair<CounterCacheKey, ClockAndCount>> deserialize(DataInputPlus in, ColumnFamilyStore cfs) throws IOException {
         if(cfs == null) {
            return null;
         } else {
            CounterCacheKey cacheKey = CounterCacheKey.read(cfs.metadata(), in);
            return cfs.metadata().isCounter() && cfs.isCounterCacheEnabled()?StageManager.getStage(Stage.BACKGROUND_IO).submit(() -> {
               ByteBuffer value = cacheKey.readCounterValue(cfs);
               return value == null?null:Pair.create(cacheKey, CounterContext.instance().getLocalClockAndCount(value));
            }):null;
         }
      }
   }

   public static enum CacheType {
      KEY_CACHE("KeyCache"),
      ROW_CACHE("RowCache"),
      COUNTER_CACHE("CounterCache");

      private final String name;

      private CacheType(String typeName) {
         this.name = typeName;
      }

      public String toString() {
         return this.name;
      }
   }
}
