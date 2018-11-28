package org.apache.cassandra.cache;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.ChecksummedRandomAccessReader;
import org.apache.cassandra.io.util.ChecksummedSequentialWriter;
import org.apache.cassandra.io.util.CorruptFileException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.LengthAvailableInputStream;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.jctools.maps.NonBlockingHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutoSavingCache<K extends CacheKey, V> extends InstrumentingCache<K, V> {
   private static final Logger logger = LoggerFactory.getLogger(AutoSavingCache.class);
   public static final Set<CacheService.CacheType> flushInProgress = new NonBlockingHashSet();
   protected volatile ScheduledFuture<?> saveTask;
   protected final CacheService.CacheType cacheType;
   private final AutoSavingCache.CacheSerializer<K, V> cacheLoader;
   private static final String CURRENT_VERSION = "f";
   private static volatile AutoSavingCache.IStreamFactory streamFactory = new AutoSavingCache.IStreamFactory() {
      private final SequentialWriterOption writerOption = SequentialWriterOption.newBuilder().trickleFsync(DatabaseDescriptor.getTrickleFsync()).trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKb() * 1024).finishOnClose(true).build();

      public InputStream getInputStream(File dataPath, File crcPath) throws IOException {
         return ChecksummedRandomAccessReader.open(dataPath, crcPath);
      }

      public OutputStream getOutputStream(File dataPath, File crcPath) {
         return new ChecksummedSequentialWriter(dataPath, crcPath, (File)null, this.writerOption);
      }
   };

   public static void setStreamFactory(AutoSavingCache.IStreamFactory streamFactory) {
      streamFactory = streamFactory;
   }

   public AutoSavingCache(ICache<K, V> cache, CacheService.CacheType cacheType, AutoSavingCache.CacheSerializer<K, V> cacheloader) {
      super(cacheType.toString(), cache);
      this.cacheType = cacheType;
      this.cacheLoader = cacheloader;
   }

   public File getCacheDataPath(String version) {
      return DatabaseDescriptor.getSerializedCachePath(this.cacheType, version, "db");
   }

   public File getCacheCrcPath(String version) {
      return DatabaseDescriptor.getSerializedCachePath(this.cacheType, version, "crc");
   }

   public AutoSavingCache<K, V>.Writer getWriter(int keysToSave) {
      return new AutoSavingCache.Writer(keysToSave);
   }

   public void scheduleSaving(int savePeriodInSeconds, final int keysToSave) {
      if(this.saveTask != null) {
         this.saveTask.cancel(false);
         this.saveTask = null;
      }

      if(savePeriodInSeconds > 0) {
         Runnable runnable = new Runnable() {
            public void run() {
               AutoSavingCache.this.submitWrite(keysToSave);
            }
         };
         this.saveTask = ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(runnable, (long)savePeriodInSeconds, (long)savePeriodInSeconds, TimeUnit.SECONDS);
      }

   }

   public ListenableFuture<Integer> loadSavedAsync() {
      final ListeningExecutorService es = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
      final long start = System.nanoTime();
      ListenableFuture<Integer> cacheLoad = es.submit(new Callable<Integer>() {
         public Integer call() {
            return Integer.valueOf(AutoSavingCache.this.loadSaved());
         }
      });
      cacheLoad.addListener(new Runnable() {
         public void run() {
            if(AutoSavingCache.this.size() > 0) {
               AutoSavingCache.logger.info("Completed loading ({} ms; {} keys) {} cache", new Object[]{Long.valueOf(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)), Integer.valueOf(AutoSavingCache.this.size()), AutoSavingCache.this.cacheType});
            }

            es.shutdown();
         }
      }, MoreExecutors.directExecutor());
      return cacheLoad;
   }

   public int loadSaved() {
      int count = 0;
      long start = System.nanoTime();
      File dataPath = this.getCacheDataPath("f");
      File crcPath = this.getCacheCrcPath("f");
      if(dataPath.exists() && crcPath.exists()) {
         DataInputPlus.DataInputStreamPlus in = null;

         try {
            logger.info("reading saved cache {}", dataPath);
            in = new DataInputPlus.DataInputStreamPlus(new LengthAvailableInputStream(new BufferedInputStream(streamFactory.getInputStream(dataPath, crcPath)), dataPath.length()));
            UUID schemaVersion = new UUID(in.readLong(), in.readLong());
            if(!schemaVersion.equals(Schema.instance.getVersion())) {
               throw new RuntimeException("Cache schema version " + schemaVersion + " does not match current schema version " + Schema.instance.getVersion());
            }

            ArrayDeque futures = new ArrayDeque();

            label187:
            while(true) {
               Future entryFuture;
               do {
                  TableId tableId;
                  if(in.available() <= 0) {
                     tableId = null;

                     Future future;
                     while((future = (Future)futures.poll()) != null) {
                        Pair<K, V> entry = (Pair)future.get();
                        if(entry != null && entry.right != null) {
                           this.put(entry.left, entry.right);
                        }
                     }
                     break label187;
                  }

                  tableId = TableId.deserialize(in);
                  String indexName = in.readUTF();
                  if(indexName.isEmpty()) {
                     indexName = null;
                  }

                  ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(tableId);
                  if(indexName != null && cfs != null) {
                     cfs = (ColumnFamilyStore)cfs.indexManager.getIndexByName(indexName).getBackingTable().orElse(null);
                  }

                  entryFuture = this.cacheLoader.deserialize(in, cfs);
               } while(entryFuture == null);

               futures.offer(entryFuture);
               ++count;

               while(true) {
                  while(futures.peek() == null || !((Future)futures.peek()).isDone()) {
                     if(futures.size() > 1000) {
                        Thread.yield();
                     }

                     if(futures.size() <= 1000) {
                        continue label187;
                     }
                  }

                  Future<Pair<K, V>> future = (Future)futures.poll();
                  Pair<K, V> entry = (Pair)future.get();
                  if(entry != null && entry.right != null) {
                     this.put(entry.left, entry.right);
                  }
               }
            }
         } catch (CorruptFileException var19) {
            JVMStabilityInspector.inspectThrowable(var19);
            logger.warn(String.format("Non-fatal checksum error reading saved cache %s", new Object[]{dataPath.getAbsolutePath()}), var19);
         } catch (Throwable var20) {
            JVMStabilityInspector.inspectThrowable(var20);
            logger.info(String.format("Harmless error reading saved cache %s", new Object[]{dataPath.getAbsolutePath()}), var20);
         } finally {
            FileUtils.closeQuietly((Closeable)in);
         }
      }

      if(logger.isTraceEnabled()) {
         logger.trace("completed reading ({} ms; {} keys) saved cache {}", new Object[]{Long.valueOf(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)), Integer.valueOf(count), dataPath});
      }

      return count;
   }

   public Future<?> submitWrite(int keysToSave) {
      return CompactionManager.instance.submitCacheWrite(this.getWriter(keysToSave));
   }

   public interface CacheSerializer<K extends CacheKey, V> {
      void serialize(K var1, DataOutputPlus var2, ColumnFamilyStore var3) throws IOException;

      Future<Pair<K, V>> deserialize(DataInputPlus var1, ColumnFamilyStore var2) throws IOException;
   }

   public class Writer extends CompactionInfo.Holder {
      private final Iterator<K> keyIterator;
      private final CompactionInfo info;
      private long keysWritten;
      private final long keysEstimate;

      protected Writer(int keysToSave) {
         int size = AutoSavingCache.this.size();
         if(keysToSave < size && keysToSave != 0) {
            this.keyIterator = AutoSavingCache.this.hotKeyIterator(keysToSave);
            this.keysEstimate = (long)keysToSave;
         } else {
            this.keyIterator = AutoSavingCache.this.keyIterator();
            this.keysEstimate = (long)size;
         }

         OperationType type;
         if(AutoSavingCache.this.cacheType == CacheService.CacheType.KEY_CACHE) {
            type = OperationType.KEY_CACHE_SAVE;
         } else if(AutoSavingCache.this.cacheType == CacheService.CacheType.ROW_CACHE) {
            type = OperationType.ROW_CACHE_SAVE;
         } else if(AutoSavingCache.this.cacheType == CacheService.CacheType.COUNTER_CACHE) {
            type = OperationType.COUNTER_CACHE_SAVE;
         } else {
            type = OperationType.UNKNOWN;
         }

         this.info = new CompactionInfo(TableMetadata.minimal("system", AutoSavingCache.this.cacheType.toString()), type, 0L, this.keysEstimate, CompactionInfo.Unit.KEYS, UUIDGen.getTimeUUID());
      }

      public CacheService.CacheType cacheType() {
         return AutoSavingCache.this.cacheType;
      }

      public CompactionInfo getCompactionInfo() {
         return this.info.forProgress(this.keysWritten, Math.max(this.keysWritten, this.keysEstimate));
      }

      public void saveCache() {
         long start;
         logger.trace("Deleting old {} files.", (Object)AutoSavingCache.this.cacheType);
         this.deleteOldCacheFiles();
         if (!this.keyIterator.hasNext()) {
            logger.trace("Skipping {} save, cache is empty.", (Object)AutoSavingCache.this.cacheType);
            return;
         }
         start = System.nanoTime();
         Pair<File, File> cacheFilePaths = this.tempCacheFiles();
         try {
            try (WrappedDataOutputStreamPlus writer = new WrappedDataOutputStreamPlus(streamFactory.getOutputStream((File)cacheFilePaths.left, (File)cacheFilePaths.right));){
               UUID schemaVersion = Schema.instance.getVersion();
               if (schemaVersion == null) {
                  TPCUtils.blockingAwait(Schema.instance.updateVersion());
                  schemaVersion = Schema.instance.getVersion();
               }
               writer.writeLong(schemaVersion.getMostSignificantBits());
               writer.writeLong(schemaVersion.getLeastSignificantBits());
               while (this.keyIterator.hasNext()) {
                  CacheKey key = (CacheKey)this.keyIterator.next();
                  ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(key.tableId);
                  if (cfs == null) continue;
                  if (key.indexName != null) {
                     cfs = cfs.indexManager.getIndexByName(key.indexName).getBackingTable().orElse(null);
                  }
                  AutoSavingCache.this.cacheLoader.serialize((K)key, writer, cfs);
                  ++this.keysWritten;
                  if (this.keysWritten < this.keysEstimate) continue;
                  break;
               }
            }
         }
         catch (FileNotFoundException e) {
            throw new RuntimeException(e);
         }
         catch (IOException e) {
            throw new FSWriteError((Throwable)e, (File)cacheFilePaths.left);
         }
         File cacheFile = AutoSavingCache.this.getCacheDataPath(AutoSavingCache.CURRENT_VERSION);
         File crcFile = AutoSavingCache.this.getCacheCrcPath(AutoSavingCache.CURRENT_VERSION);
         cacheFile.delete();
         crcFile.delete();
         if (!((File)cacheFilePaths.left).renameTo(cacheFile)) {
            logger.error("Unable to rename {} to {}", cacheFilePaths.left, (Object)cacheFile);
         }
         if (!((File)cacheFilePaths.right).renameTo(crcFile)) {
            logger.error("Unable to rename {} to {}", cacheFilePaths.right, (Object)crcFile);
         }
         logger.info("Saved {} ({} items) in {} ms", new Object[]{AutoSavingCache.this.cacheType, this.keysWritten, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)});
      }

      private Pair<File, File> tempCacheFiles() {
         File dataPath = AutoSavingCache.this.getCacheDataPath("f");
         File crcPath = AutoSavingCache.this.getCacheCrcPath("f");
         return Pair.create(FileUtils.createTempFile(dataPath.getName(), (String)null, dataPath.getParentFile()), FileUtils.createTempFile(crcPath.getName(), (String)null, crcPath.getParentFile()));
      }

      private void deleteOldCacheFiles() {
         File savedCachesDir = new File(DatabaseDescriptor.getSavedCachesLocation());

         assert savedCachesDir.exists() && savedCachesDir.isDirectory();

         File[] files = savedCachesDir.listFiles();
         if(files != null) {
            String cacheNameFormat = String.format("%s-%s.db", new Object[]{AutoSavingCache.this.cacheType.toString(), "f"});
            File[] var4 = files;
            int var5 = files.length;

            for(int var6 = 0; var6 < var5; ++var6) {
               File file = var4[var6];
               if(file.isFile() && (file.getName().endsWith(cacheNameFormat) || file.getName().endsWith(AutoSavingCache.this.cacheType.toString())) && !file.delete()) {
                  AutoSavingCache.logger.warn("Failed to delete {}", file.getAbsolutePath());
               }
            }
         } else {
            AutoSavingCache.logger.warn("Could not list files in {}", savedCachesDir);
         }

      }
   }

   public interface IStreamFactory {
      InputStream getInputStream(File var1, File var2) throws IOException;

      OutputStream getOutputStream(File var1, File var2) throws FileNotFoundException;
   }
}
