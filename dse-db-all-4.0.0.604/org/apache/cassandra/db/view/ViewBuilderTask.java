package org.apache.cassandra.db.view;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Refs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ViewBuilderTask extends CompactionInfo.Holder implements Callable<Long> {
   private static final Logger logger = LoggerFactory.getLogger(ViewBuilderTask.class);
   private static final int ROWS_BETWEEN_CHECKPOINTS = 1000;
   private final ColumnFamilyStore baseCfs;
   private final View view;
   private final Range<Token> range;
   private final UUID compactionId;
   private volatile Token prevToken;
   private volatile long keysBuilt = 0L;
   private volatile boolean isStopped = false;
   private volatile boolean isCompactionInterrupted = false;

   ViewBuilderTask(ColumnFamilyStore baseCfs, View view, Range<Token> range, Token lastToken, long keysBuilt) {
      this.baseCfs = baseCfs;
      this.view = view;
      this.range = range;
      this.compactionId = UUIDGen.getTimeUUID();
      this.prevToken = lastToken;
      this.keysBuilt = keysBuilt;
   }

   private void buildKey(DecoratedKey key) {
      ReadQuery selectQuery = this.view.getReadQuery();
      if(!selectQuery.selectsKey(key)) {
         logger.trace("Skipping {}, view query filters", key);
      } else {
         int nowInSec = FBUtilities.nowInSeconds();
         SinglePartitionReadCommand command = this.view.getSelectStatement().internalReadForView(key, nowInSec);
         UnfilteredRowIterator data = UnfilteredPartitionIterators.getOnlyElement(command.executeForTests(), command);
         Throwable var6 = null;

         try {
            Iterator<Collection<Mutation>> mutations = this.baseCfs.keyspace.viewManager.forTable(this.baseCfs.metadata.id).generateViewUpdates(Collections.singleton(this.view), data, nowInSec);
            AtomicLong noBase = new AtomicLong(9223372036854775807L);
            mutations.forEachRemaining((m) -> {
               StorageProxy.mutateMV(key.getKey(), m, true, noBase, System.nanoTime()).blockingAwait();
            });
         } catch (Throwable var16) {
            var6 = var16;
            throw var16;
         } finally {
            if(data != null) {
               if(var6 != null) {
                  try {
                     data.close();
                  } catch (Throwable var15) {
                     var6.addSuppressed(var15);
                  }
               } else {
                  data.close();
               }
            }

         }

      }
   }

   public Long call() {
      String ksName = this.baseCfs.metadata.keyspace;
      if(this.prevToken == null) {
         logger.debug("Starting new view build for range {}", this.range);
      } else {
         logger.debug("Resuming view build for range {} from token {} with {} covered keys", new Object[]{this.range, this.prevToken, Long.valueOf(this.keysBuilt)});
      }

      Function<org.apache.cassandra.db.lifecycle.View, Iterable<SSTableReader>> function = org.apache.cassandra.db.lifecycle.View.select(SSTableSet.CANONICAL, (s) -> {
         return this.range.intersects(s.getBounds());
      });
      ColumnFamilyStore.RefViewFragment viewFragment = this.baseCfs.selectAndReference(function);
      Throwable var4 = null;

      try {
         Refs<SSTableReader> sstables = viewFragment.refs;
         Throwable var6 = null;

         try {
            ReducingKeyIterator keyIter = new ReducingKeyIterator(sstables);
            Throwable var8 = null;

            try {
               PeekingIterator iter = Iterators.peekingIterator(keyIter);

               label501:
               while(true) {
                  DecoratedKey key;
                  Token token;
                  do {
                     do {
                        if(this.isStopped || !iter.hasNext()) {
                           break label501;
                        }

                        key = (DecoratedKey)iter.next();
                        token = key.getToken();
                     } while(!this.range.contains((RingPosition)token));
                  } while(this.prevToken != null && token.compareTo(this.prevToken) <= 0);

                  this.buildKey(key);
                  ++this.keysBuilt;

                  while(iter.hasNext() && ((DecoratedKey)iter.peek()).getToken().equals(token)) {
                     key = (DecoratedKey)iter.next();
                     this.buildKey(key);
                     ++this.keysBuilt;
                  }

                  if(this.keysBuilt % 1000L == 1L) {
                     this.updateViewBuildStatus(ksName, token);
                  }

                  this.prevToken = token;
               }
            } catch (Throwable var54) {
               var8 = var54;
               throw var54;
            } finally {
               if(keyIter != null) {
                  if(var8 != null) {
                     try {
                        keyIter.close();
                     } catch (Throwable var53) {
                        var8.addSuppressed(var53);
                     }
                  } else {
                     keyIter.close();
                  }
               }

            }
         } catch (Throwable var56) {
            var6 = var56;
            throw var56;
         } finally {
            if(sstables != null) {
               if(var6 != null) {
                  try {
                     sstables.close();
                  } catch (Throwable var52) {
                     var6.addSuppressed(var52);
                  }
               } else {
                  sstables.close();
               }
            }

         }
      } catch (Throwable var58) {
         var4 = var58;
         throw var58;
      } finally {
         if(viewFragment != null) {
            if(var4 != null) {
               try {
                  viewFragment.close();
               } catch (Throwable var51) {
                  var4.addSuppressed(var51);
               }
            } else {
               viewFragment.close();
            }
         }

      }

      this.finish();
      return Long.valueOf(this.keysBuilt);
   }

   private void updateViewBuildStatus(String ksName, Token token) {
      try {
         SystemKeyspace.updateViewBuildStatus(ksName, this.view.name, this.range, token, this.keysBuilt).get();
      } catch (Exception var4) {
         logger.warn("Problem while setting view build status of view {}.{}", new Object[]{ksName, this.view.name, var4});
      }

   }

   private void finish() {
      String ksName = this.baseCfs.keyspace.getName();
      if(!this.isStopped) {
         this.updateViewBuildStatus(ksName, (Token)this.range.right);
         logger.debug("Completed build of view({}.{}) for range {} after covering {} keys ", new Object[]{ksName, this.view.name, this.range, Long.valueOf(this.keysBuilt)});
      } else {
         logger.debug("Stopped build for view({}.{}) for range {} after covering {} keys", new Object[]{ksName, this.view.name, this.range, Long.valueOf(this.keysBuilt)});
         if(this.isCompactionInterrupted) {
            throw new ViewBuilderTask.StoppedException(ksName, this.view.name, this.getCompactionInfo());
         }
      }

   }

   public CompactionInfo getCompactionInfo() {
      long keysTotal;
      if(((Token)this.range.left).getPartitioner().splitter().isPresent()) {
         keysTotal = this.prevToken == null?0L:Math.round(((Splitter)this.prevToken.getPartitioner().splitter().get()).positionInRange(this.prevToken, this.range) * 1000.0D);
         return new CompactionInfo(this.baseCfs.metadata(), OperationType.VIEW_BUILD, keysTotal, 1000L, CompactionInfo.Unit.RANGES, this.compactionId);
      } else {
         keysTotal = Math.max(this.keysBuilt + 1L, this.baseCfs.estimatedKeysForRange(this.range));
         return new CompactionInfo(this.baseCfs.metadata(), OperationType.VIEW_BUILD, this.keysBuilt, keysTotal, CompactionInfo.Unit.KEYS, this.compactionId);
      }
   }

   public void stop() {
      this.stop(true);
   }

   synchronized void stop(boolean isCompactionInterrupted) {
      this.isStopped = true;
      this.isCompactionInterrupted = isCompactionInterrupted;
   }

   long keysBuilt() {
      return this.keysBuilt;
   }

   static class StoppedException extends CompactionInterruptedException {
      private final String ksName;
      private final String viewName;

      private StoppedException(String ksName, String viewName, CompactionInfo info) {
         super(info);
         this.ksName = ksName;
         this.viewName = viewName;
      }

      public boolean equals(Object o) {
         if(!(o instanceof ViewBuilderTask.StoppedException)) {
            return false;
         } else {
            ViewBuilderTask.StoppedException that = (ViewBuilderTask.StoppedException)o;
            return Objects.equal(this.ksName, that.ksName) && Objects.equal(this.viewName, that.viewName);
         }
      }

      public int hashCode() {
         return 31 * this.ksName.hashCode() + this.viewName.hashCode();
      }
   }
}
