package org.apache.cassandra.db.lifecycle;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.MemtableDiscardedNotification;
import org.apache.cassandra.notifications.MemtableRenewedNotification;
import org.apache.cassandra.notifications.MemtableSwitchedNotification;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableDeletingNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;
import org.apache.cassandra.notifications.TruncationNotification;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Refs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Tracker {
   private static final Logger logger = LoggerFactory.getLogger(Tracker.class);
   private final Collection<INotificationConsumer> subscribers = new CopyOnWriteArrayList();
   public final ColumnFamilyStore cfstore;
   final AtomicReference<View> view;
   public final boolean loadsstables;
   private final Map<UUID, LifecycleTransaction> activeTransactions;

   public Tracker(Memtable memtable, boolean loadsstables) {
      this.cfstore = memtable != null?memtable.cfs:null;
      this.view = new AtomicReference();
      this.loadsstables = loadsstables;
      this.reset(memtable);
      this.activeTransactions = new ConcurrentHashMap();
   }

   public LifecycleTransaction tryModify(SSTableReader sstable, OperationType operationType) {
      return this.tryModify((Iterable)Collections.singleton(sstable), operationType);
   }

   public LifecycleTransaction tryModify(Iterable<SSTableReader> sstables, OperationType operationType) {
      if(!Iterables.isEmpty(sstables) && this.apply(View.permitCompacting(sstables), View.updateCompacting(emptySet(), sstables)) == null) {
         return null;
      } else {
         LifecycleTransaction txn = new LifecycleTransaction(this, operationType, sstables);
         this.activeTransactions.put(txn.opId(), txn);
         return txn;
      }
   }

   public LifecycleTransaction finishTransaction(UUID id) {
      return (LifecycleTransaction)this.activeTransactions.remove(id);
   }

   public Set<LifecycleTransaction> getTransactions() {
      return new HashSet(this.activeTransactions.values());
   }

   Pair<View, View> apply(Function<View, View> function) {
      return this.apply(Predicates.alwaysTrue(), function);
   }

   Throwable apply(Function<View, View> function, Throwable accumulate) {
      try {
         this.apply(function);
      } catch (Throwable var4) {
         accumulate = Throwables.merge(accumulate, var4);
      }

      return accumulate;
   }

   Pair<View, View> apply(Predicate<View> permit, Function<View, View> function) {
      View cur;
      View updated;
      do {
         cur = (View)this.view.get();
         if(!permit.apply(cur)) {
            return null;
         }

         updated = (View)function.apply(cur);
      } while(!this.view.compareAndSet(cur, updated));

      return Pair.create(cur, updated);
   }

   Throwable updateSizeTracking(Iterable<SSTableReader> oldSSTables, Iterable<SSTableReader> newSSTables, Throwable accumulate) {
      if(this.isDummy()) {
         return accumulate;
      } else {
         long add = 0L;
         Iterator var6 = newSSTables.iterator();

         while(var6.hasNext()) {
            SSTableReader sstable = (SSTableReader)var6.next();
            if(logger.isTraceEnabled()) {
               logger.trace("adding {} to list of files tracked for {}.{}", new Object[]{sstable.descriptor, this.cfstore.keyspace.getName(), this.cfstore.name});
            }

            try {
               add += sstable.bytesOnDisk();
            } catch (Throwable var12) {
               accumulate = Throwables.merge(accumulate, var12);
            }
         }

         long subtract = 0L;
         Iterator var8 = oldSSTables.iterator();

         while(var8.hasNext()) {
            SSTableReader sstable = (SSTableReader)var8.next();
            if(logger.isTraceEnabled()) {
               logger.trace("removing {} from list of files tracked for {}.{}", new Object[]{sstable.descriptor, this.cfstore.keyspace.getName(), this.cfstore.name});
            }

            try {
               subtract += sstable.bytesOnDisk();
            } catch (Throwable var11) {
               accumulate = Throwables.merge(accumulate, var11);
            }
         }

         StorageMetrics.load.inc(add - subtract);
         this.cfstore.metric.liveDiskSpaceUsed.inc(add - subtract);
         this.cfstore.metric.totalDiskSpaceUsed.inc(add);
         return accumulate;
      }
   }

   public void addInitialSSTables(Iterable<SSTableReader> sstables) {
      if(!this.isDummy()) {
         Helpers.setupOnline(sstables);
      }

      this.apply(View.updateLiveSet(emptySet(), sstables));
      Throwables.maybeFail(this.updateSizeTracking(emptySet(), sstables, (Throwable)null));
   }

   public void addSSTables(Iterable<SSTableReader> sstables) {
      this.addInitialSSTables(sstables);
      this.maybeIncrementallyBackup(sstables);
      this.notifyAdded(sstables);
   }

   public void addSSTablesFromStreaming(Iterable<SSTableReader> sstables) {
      this.addInitialSSTables(sstables);
      this.maybeIncrementallyBackup(sstables);
      this.notifyAddedFromStreaming(sstables);
   }

   @VisibleForTesting
   public void reset(Memtable memtable) {
      this.view.set(new View(memtable != null?Collections.singletonList(memtable):Collections.emptyList(), Collections.emptyList(), Collections.emptyMap(), Collections.emptyMap(), SSTableIntervalTree.empty()));
   }

   @VisibleForTesting
   public void removeSSTablesFromTrackerUnsafe(Collection<SSTableReader> sstablesToRemove) {
      View currentView = (View)this.view.get();
      Set<SSTableReader> toRemove = new HashSet(sstablesToRemove);
      Map<SSTableReader, SSTableReader> sstables = new HashMap(currentView.sstablesMap);
      Iterator var5 = toRemove.iterator();

      while(var5.hasNext()) {
         SSTableReader sstable = (SSTableReader)var5.next();
         sstables.remove(sstable);
      }

      this.view.set(new View(currentView.liveMemtables, currentView.flushingMemtables, sstables, currentView.compactingMap, currentView.intervalTree));
      this.notifySSTablesChanged(sstablesToRemove, Collections.emptyList(), OperationType.UNKNOWN, (Throwable)null);
   }

   public Throwable dropSSTablesIfInvalid(Throwable accumulate) {
      if(!this.isDummy() && !this.cfstore.isValid()) {
         accumulate = this.dropSSTables(accumulate);
      }

      return accumulate;
   }

   public void dropSSTables() {
      Throwables.maybeFail(this.dropSSTables((Throwable)null));
   }

   public Throwable dropSSTables(Throwable accumulate) {
      return this.dropSSTables(Predicates.alwaysTrue(), OperationType.UNKNOWN, accumulate);
   }

   public Throwable dropSSTables(Predicate<SSTableReader> remove, OperationType operationType, Throwable accumulate) {
      try {
         LogTransaction txnLogs = new LogTransaction(operationType, this);
         Throwable var5 = null;

         try {
            Pair<View, View> result = this.apply((view) -> {
               Set<SSTableReader> toremove = ImmutableSet.copyOf(Iterables.filter(view.sstables, Predicates.and(remove, Helpers.notIn(new Set[]{view.compacting}))));
               return (View)View.updateLiveSet(toremove, emptySet()).apply(view);
            });
            Set<SSTableReader> removed = Sets.difference(((View)result.left).sstables, ((View)result.right).sstables);

            assert Iterables.all(removed, remove);

            List<LogTransaction.Obsoletion> obsoletions = new ArrayList();
            accumulate = Helpers.prepareForObsoletion(removed, txnLogs, obsoletions, accumulate);

            try {
               txnLogs.finish();
               if(!removed.isEmpty()) {
                  accumulate = Helpers.markObsolete(obsoletions, accumulate);
                  accumulate = this.updateSizeTracking(removed, emptySet(), accumulate);
                  accumulate = Refs.release(Refs.selfRefs(removed), accumulate);
                  accumulate = this.notifySSTablesChanged(removed, Collections.emptySet(), txnLogs.type(), accumulate);
               }
            } catch (Throwable var19) {
               accumulate = Helpers.abortObsoletion(obsoletions, accumulate);
               accumulate = Throwables.merge(accumulate, var19);
            }
         } catch (Throwable var20) {
            var5 = var20;
            throw var20;
         } finally {
            if(txnLogs != null) {
               if(var5 != null) {
                  try {
                     txnLogs.close();
                  } catch (Throwable var18) {
                     var5.addSuppressed(var18);
                  }
               } else {
                  txnLogs.close();
               }
            }

         }
      } catch (Throwable var22) {
         accumulate = Throwables.merge(accumulate, var22);
      }

      return accumulate;
   }

   public void removeUnreadableSSTables(final File directory) {
      Throwables.maybeFail(this.dropSSTables(new Predicate<SSTableReader>() {
         public boolean apply(SSTableReader reader) {
            return reader.descriptor.directory.equals(directory);
         }
      }, OperationType.UNKNOWN, (Throwable)null));
   }

   public Memtable getMemtableFor(OpOrder.Group opGroup, CommitLogPosition commitLogPosition) {
      Iterator var3 = ((View)this.view.get()).liveMemtables.iterator();

      Memtable memtable;
      do {
         if(!var3.hasNext()) {
            throw new AssertionError(((View)this.view.get()).liveMemtables.toString());
         }

         memtable = (Memtable)var3.next();
      } while(!memtable.accepts(opGroup, commitLogPosition));

      return memtable;
   }

   public Memtable switchMemtable(boolean truncating, Memtable newMemtable) {
      Pair<View, View> result = this.apply(View.switchMemtable(newMemtable));
      if(truncating) {
         this.notifyRenewed(newMemtable);
      } else {
         this.notifySwitched(((View)result.left).getCurrentMemtable());
      }

      return ((View)result.left).getCurrentMemtable();
   }

   public void markFlushing(Memtable memtable) {
      this.apply(View.markFlushing(memtable));
   }

   public void replaceFlushed(Memtable memtable, Iterable<SSTableReader> sstables) {
      assert !this.isDummy();

      if(Iterables.isEmpty(sstables)) {
         this.apply(View.replaceFlushed(memtable, (Iterable)null));
      } else {
         sstables.forEach(SSTableReader::setupOnline);
         this.maybeIncrementallyBackup(sstables);
         this.apply(View.replaceFlushed(memtable, sstables));
         Throwable fail = this.updateSizeTracking(emptySet(), sstables, (Throwable)null);
         this.notifyDiscarded(memtable);
         fail = this.notifyAdded(sstables, memtable, fail, false);
         if(!this.isDummy() && !this.cfstore.isValid()) {
            this.dropSSTables();
         }

         Throwables.maybeFail(fail);
      }
   }

   public Set<SSTableReader> getCompacting() {
      return ((View)this.view.get()).compacting;
   }

   public Iterable<SSTableReader> getUncompacting() {
      return ((View)this.view.get()).select(SSTableSet.NONCOMPACTING);
   }

   public Iterable<SSTableReader> getUncompacting(Iterable<SSTableReader> candidates) {
      return ((View)this.view.get()).getUncompacting(candidates);
   }

   public void maybeIncrementallyBackup(Iterable<SSTableReader> sstables) {
      if(DatabaseDescriptor.isIncrementalBackupsEnabled()) {
         Iterator var2 = sstables.iterator();

         while(var2.hasNext()) {
            SSTableReader sstable = (SSTableReader)var2.next();
            File backupsDir = Directories.getBackupsDirectory(sstable.descriptor);
            sstable.createLinks(FileUtils.getCanonicalPath(backupsDir));
         }

      }
   }

   Throwable notifySSTablesChanged(Collection<SSTableReader> removed, Collection<SSTableReader> added, OperationType compactionType, Throwable accumulate) {
      INotification notification = new SSTableListChangedNotification(added, removed, compactionType);
      Iterator var6 = this.subscribers.iterator();

      while(var6.hasNext()) {
         INotificationConsumer subscriber = (INotificationConsumer)var6.next();

         try {
            subscriber.handleNotification(notification, this);
         } catch (Throwable var9) {
            accumulate = Throwables.merge(accumulate, var9);
         }
      }

      return accumulate;
   }

   Throwable notifyAdded(Iterable<SSTableReader> added, Memtable memtable, Throwable accumulate, boolean fromStreaming) {
      INotification notification = new SSTableAddedNotification(added, memtable, fromStreaming);
      Iterator var6 = this.subscribers.iterator();

      while(var6.hasNext()) {
         INotificationConsumer subscriber = (INotificationConsumer)var6.next();

         try {
            subscriber.handleNotification(notification, this);
         } catch (Throwable var9) {
            accumulate = Throwables.merge(accumulate, var9);
         }
      }

      return accumulate;
   }

   public void notifyAdded(Iterable<SSTableReader> added) {
      Throwables.maybeFail(this.notifyAdded(added, (Memtable)null, (Throwable)null, false));
   }

   public void notifyAddedFromStreaming(Iterable<SSTableReader> added) {
      Throwables.maybeFail(this.notifyAdded(added, (Memtable)null, (Throwable)null, true));
   }

   public void notifySSTableRepairedStatusChanged(Collection<SSTableReader> repairStatusesChanged) {
      INotification notification = new SSTableRepairStatusChanged(repairStatusesChanged);
      Iterator var3 = this.subscribers.iterator();

      while(var3.hasNext()) {
         INotificationConsumer subscriber = (INotificationConsumer)var3.next();
         subscriber.handleNotification(notification, this);
      }

   }

   public void notifyDeleting(SSTableReader deleting) {
      INotification notification = new SSTableDeletingNotification(deleting);
      Iterator var3 = this.subscribers.iterator();

      while(var3.hasNext()) {
         INotificationConsumer subscriber = (INotificationConsumer)var3.next();
         subscriber.handleNotification(notification, this);
      }

   }

   public void notifyTruncated(long truncatedAt) {
      INotification notification = new TruncationNotification(truncatedAt);
      Iterator var4 = this.subscribers.iterator();

      while(var4.hasNext()) {
         INotificationConsumer subscriber = (INotificationConsumer)var4.next();
         subscriber.handleNotification(notification, this);
      }

   }

   public void notifyRenewed(Memtable renewed) {
      this.notify(new MemtableRenewedNotification(renewed));
   }

   public void notifySwitched(Memtable previous) {
      this.notify(new MemtableSwitchedNotification(previous));
   }

   public void notifyDiscarded(Memtable discarded) {
      this.notify(new MemtableDiscardedNotification(discarded));
   }

   private void notify(INotification notification) {
      Iterator var2 = this.subscribers.iterator();

      while(var2.hasNext()) {
         INotificationConsumer subscriber = (INotificationConsumer)var2.next();
         subscriber.handleNotification(notification, this);
      }

   }

   public boolean isDummy() {
      return this.cfstore == null || !DatabaseDescriptor.isDaemonInitialized();
   }

   public void subscribe(INotificationConsumer consumer) {
      this.subscribers.add(consumer);
   }

   public void unsubscribe(INotificationConsumer consumer) {
      this.subscribers.remove(consumer);
   }

   private static Set<SSTableReader> emptySet() {
      return Collections.emptySet();
   }

   public View getView() {
      return (View)this.view.get();
   }

   @VisibleForTesting
   public void removeUnsafe(Set<SSTableReader> toRemove) {
      this.apply((view) -> {
         return (View)View.updateLiveSet(toRemove, emptySet()).apply(view);
      });
   }
}
