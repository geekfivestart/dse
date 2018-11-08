package org.apache.cassandra.io.sstable;

import com.google.common.annotations.VisibleForTesting;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import org.apache.cassandra.cache.InstrumentingCache;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.cache.MutableKeyCacheKey;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.big.BigTableReader;
import org.apache.cassandra.utils.concurrent.Transactional;

public class SSTableRewriter extends Transactional.AbstractTransactional implements Transactional {
   @VisibleForTesting
   public static boolean disableEarlyOpeningForTests = false;
   private static boolean disableIncrementalKeyCacheMigration = Boolean.getBoolean("cassandra.disable_incremental_keycache_migration");
   private final long preemptiveOpenInterval;
   private final long maxAge;
   private long repairedAt;
   private final LifecycleTransaction transaction;
   private final List<SSTableReader> preparedForCommit;
   private long currentlyOpenedEarlyAt;
   private final List<SSTableWriter> writers;
   private final boolean keepOriginals;
   private MutableKeyCacheKey tmpKey;
   private SSTableWriter writer;
   private Collection<DecoratedKey> cachedKeys;
   private boolean throwEarly;
   private boolean throwLate;

   /** @deprecated */
   @Deprecated
   public SSTableRewriter(LifecycleTransaction transaction, long maxAge, boolean isOffline) {
      this(transaction, maxAge, isOffline, true);
   }

   /** @deprecated */
   @Deprecated
   public SSTableRewriter(LifecycleTransaction transaction, long maxAge, boolean isOffline, boolean shouldOpenEarly) {
      this(transaction, maxAge, calculateOpenInterval(shouldOpenEarly), false);
   }

   @VisibleForTesting
   public SSTableRewriter(LifecycleTransaction transaction, long maxAge, long preemptiveOpenInterval, boolean keepOriginals) {
      this.repairedAt = -1L;
      this.preparedForCommit = new ArrayList();
      this.writers = new ArrayList();
      this.cachedKeys = new ArrayList();
      this.transaction = transaction;
      this.maxAge = maxAge;
      this.keepOriginals = keepOriginals;
      this.preemptiveOpenInterval = preemptiveOpenInterval;
   }

   /** @deprecated */
   @Deprecated
   public static SSTableRewriter constructKeepingOriginals(LifecycleTransaction transaction, boolean keepOriginals, long maxAge, boolean isOffline) {
      return constructKeepingOriginals(transaction, keepOriginals, maxAge);
   }

   public static SSTableRewriter constructKeepingOriginals(LifecycleTransaction transaction, boolean keepOriginals, long maxAge) {
      return new SSTableRewriter(transaction, maxAge, calculateOpenInterval(true), keepOriginals);
   }

   public static SSTableRewriter constructWithoutEarlyOpening(LifecycleTransaction transaction, boolean keepOriginals, long maxAge) {
      return new SSTableRewriter(transaction, maxAge, calculateOpenInterval(false), keepOriginals);
   }

   public static SSTableRewriter construct(ColumnFamilyStore cfs, LifecycleTransaction transaction, boolean keepOriginals, long maxAge) {
      return new SSTableRewriter(transaction, maxAge, calculateOpenInterval(cfs.supportsEarlyOpen()), keepOriginals);
   }

   private static long calculateOpenInterval(boolean shouldOpenEarly) {
      long interval = (long)DatabaseDescriptor.getSSTablePreempiveOpenIntervalInMB();
      return !disableEarlyOpeningForTests && shouldOpenEarly && interval > 0L?interval * 1048576L:9223372036854775807L;
   }

   public SSTableWriter currentWriter() {
      return this.writer;
   }

   public RowIndexEntry append(UnfilteredRowIterator partition) {
      this.maybeReopenEarly();
      RowIndexEntry index = this.writer.append(partition);
      this.prepForKeyCacheInvalidation(partition);
      return index;
   }

   public RowIndexEntry tryAppend(UnfilteredRowIterator partition) {
      this.writer.mark();

      try {
         return this.append(partition);
      } catch (Throwable var3) {
         this.writer.resetAndTruncate();
         throw var3;
      }
   }

   private void maybeReopenEarly() {
      if(this.writer.getFilePointer() - this.currentlyOpenedEarlyAt > this.preemptiveOpenInterval) {
         if(!this.transaction.isOffline()) {
            this.writer.setMaxDataAge(this.maxAge).openEarly((reader) -> {
               this.transaction.update(reader, false);
               this.moveStarts(reader, reader.last);
               this.transaction.checkpoint();
            });
         }

         this.currentlyOpenedEarlyAt = this.writer.getFilePointer();
      }

   }

   protected Throwable doAbort(Throwable accumulate) {
      SSTableWriter writer;
      for(Iterator var2 = this.writers.iterator(); var2.hasNext(); accumulate = writer.abort(accumulate)) {
         writer = (SSTableWriter)var2.next();
      }

      accumulate = this.transaction.abort(accumulate);
      return accumulate;
   }

   protected Throwable doCommit(Throwable accumulate) {
      SSTableWriter writer;
      for(Iterator var2 = this.writers.iterator(); var2.hasNext(); accumulate = writer.commit(accumulate)) {
         writer = (SSTableWriter)var2.next();
      }

      accumulate = this.transaction.commit(accumulate);
      return accumulate;
   }

   private void moveStarts(SSTableReader newReader, DecoratedKey lowerbound) {
      if(!this.transaction.isOffline()) {
         if(this.preemptiveOpenInterval != 9223372036854775807L) {
            newReader.setupOnline();
            Iterator var3 = this.transaction.originals().iterator();

            while(var3.hasNext()) {
               SSTableReader sstable = (SSTableReader)var3.next();
               SSTableReader latest = this.transaction.current(sstable);
               if(latest.first.compareTo((PartitionPosition)lowerbound) <= 0) {
                  if(lowerbound.compareTo((PartitionPosition)latest.last) >= 0) {
                     if(!this.transaction.isObsolete(latest)) {
                        this.scheduleKeyCacheInvalidation(latest);
                        this.transaction.obsolete(latest);
                     }
                  } else {
                     this.cachedKeys.clear();
                     DecoratedKey newStart = latest.firstKeyBeyond(lowerbound);

                     assert newStart != null;

                     SSTableReader replacement = latest.cloneWithNewStart(newStart);
                     this.transaction.update(replacement, true);
                  }
               }
            }

         }
      }
   }

   private void scheduleKeyCacheInvalidation(SSTableReader table) {
      if(!this.cachedKeys.isEmpty() && table instanceof BigTableReader) {
         table.runOnClose(new SSTableRewriter.InvalidateKeys((BigTableReader)table, this.cachedKeys));
      }

   }

   private void prepForKeyCacheInvalidation(UnfilteredRowIterator partition) {
      if(!this.transaction.isOffline() && this.preemptiveOpenInterval != 9223372036854775807L && !disableIncrementalKeyCacheMigration) {
         Iterator var2 = this.transaction.originals().iterator();

         while(var2.hasNext()) {
            SSTableReader rdr = (SSTableReader)var2.next();
            if(rdr instanceof BigTableReader) {
               BigTableReader reader = (BigTableReader)rdr;
               DecoratedKey key = partition.partitionKey();
               if(key.getToken().compareTo(reader.first.getToken()) >= 0 && key.getToken().compareTo(reader.last.getToken()) <= 0) {
                  if(this.tmpKey == null) {
                     this.tmpKey = new MutableKeyCacheKey(reader.metadata(), reader.descriptor, key.getKey());
                  } else {
                     this.tmpKey.mutate(reader.descriptor, key.getKey());
                  }

                  if(reader.getCachedPosition((KeyCacheKey)this.tmpKey, false) != null) {
                     this.cachedKeys.add(key);
                     break;
                  }
               }
            }
         }
      }

   }

   public void switchWriter(SSTableWriter newWriter) {
      if(newWriter != null) {
         this.writers.add(newWriter.setMaxDataAge(this.maxAge));
      }

      if(this.writer != null && this.writer.getFilePointer() != 0L) {
         if(this.preemptiveOpenInterval != 9223372036854775807L) {
            SSTableReader reader = this.writer.setMaxDataAge(this.maxAge).openFinalEarly();
            this.transaction.update(reader, false);
            this.moveStarts(reader, reader.last);
            this.transaction.checkpoint();
         }

         this.currentlyOpenedEarlyAt = 0L;
         this.writer = newWriter;
      } else {
         if(this.writer != null) {
            this.writer.abort();
            this.transaction.untrackNew(this.writer);
            this.writers.remove(this.writer);
         }

         this.writer = newWriter;
      }
   }

   public SSTableRewriter setRepairedAt(long repairedAt) {
      this.repairedAt = repairedAt;
      return this;
   }

   public List<SSTableReader> finish() {
      super.finish();
      return this.finished();
   }

   public List<SSTableReader> finished() {
      assert this.state() == Transactional.AbstractTransactional.State.COMMITTED || this.state() == Transactional.AbstractTransactional.State.READY_TO_COMMIT;

      return this.preparedForCommit;
   }

   protected void doPrepare() {
      this.switchWriter((SSTableWriter)null);
      if(this.throwEarly) {
         throw new RuntimeException("exception thrown early in finish, for testing");
      } else {
         Iterator var1 = this.writers.iterator();

         while(var1.hasNext()) {
            SSTableWriter writer = (SSTableWriter)var1.next();

            assert writer.getFilePointer() > 0L;

            writer.setRepairedAt(this.repairedAt).setOpenResult(true).prepareToCommit();
            SSTableReader reader = writer.finished();
            this.transaction.update(reader, false);
            this.preparedForCommit.add(reader);
         }

         this.transaction.checkpoint();
         if(this.throwLate) {
            throw new RuntimeException("exception thrown after all sstables finished, for testing");
         } else {
            if(!this.keepOriginals) {
               this.transaction.obsoleteOriginals();
            }

            this.transaction.prepareToCommit();
         }
      }
   }

   public void throwDuringPrepare(boolean earlyException) {
      if(earlyException) {
         this.throwEarly = true;
      } else {
         this.throwLate = true;
      }

   }

   private static final class InvalidateKeys implements AutoCloseable {
      final List<KeyCacheKey> cacheKeys;
      final WeakReference<InstrumentingCache<KeyCacheKey, ?>> cacheRef;

      private InvalidateKeys(BigTableReader reader, Collection<DecoratedKey> invalidate) {
         this.cacheKeys = new ArrayList();
         this.cacheRef = new WeakReference(reader.getKeyCache());
         if(this.cacheRef.get() != null) {
            Iterator var3 = invalidate.iterator();

            while(var3.hasNext()) {
               DecoratedKey key = (DecoratedKey)var3.next();
               this.cacheKeys.add(reader.getCacheKey(key));
            }
         }

      }

      public void close() {
         Iterator var1 = this.cacheKeys.iterator();

         while(var1.hasNext()) {
            KeyCacheKey key = (KeyCacheKey)var1.next();
            InstrumentingCache<KeyCacheKey, ?> cache = (InstrumentingCache)this.cacheRef.get();
            if(cache != null) {
               cache.remove(key);
            }
         }

      }
   }
}
