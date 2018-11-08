package org.apache.cassandra.db.compaction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableDeletingNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionStrategyManager implements INotificationConsumer {
   private static final Logger logger = LoggerFactory.getLogger(CompactionStrategyManager.class);
   public final CompactionLogger compactionLogger;
   private final ColumnFamilyStore cfs;
   private final boolean partitionSSTablesByTokenRange;
   private final Supplier<DiskBoundaries> boundariesSupplier;
   private final ReentrantReadWriteLock lock;
   private final ReadLock readLock;
   private final WriteLock writeLock;
   private final List<AbstractCompactionStrategy> repaired;
   private final List<AbstractCompactionStrategy> unrepaired;
   private final List<PendingRepairManager> pendingRepairs;
   private volatile CompactionParams params;
   private DiskBoundaries currentBoundaries;
   private volatile boolean enabled;
   private volatile boolean isActive;
   private volatile CompactionParams schemaCompactionParams;
   private boolean shouldDefragment;
   private boolean supportsEarlyOpen;
   private int fanout;

   public CompactionStrategyManager(ColumnFamilyStore cfs) {
      this(cfs, cfs::getDiskBoundaries, cfs.getPartitioner().splitter().isPresent());
   }

   @VisibleForTesting
   public CompactionStrategyManager(ColumnFamilyStore cfs, Supplier<DiskBoundaries> boundariesSupplier, boolean partitionSSTablesByTokenRange) {
      this.lock = new ReentrantReadWriteLock();
      this.readLock = this.lock.readLock();
      this.writeLock = this.lock.writeLock();
      this.repaired = new ArrayList();
      this.unrepaired = new ArrayList();
      this.pendingRepairs = new ArrayList();
      this.enabled = true;
      this.isActive = true;
      cfs.getTracker().subscribe(this);
      logger.trace("{} subscribed to the data tracker.", this);
      this.cfs = cfs;
      this.compactionLogger = new CompactionLogger(cfs, this);
      this.boundariesSupplier = boundariesSupplier;
      this.partitionSSTablesByTokenRange = partitionSSTablesByTokenRange;
      this.params = cfs.metadata().params.compaction;
      this.enabled = this.params.isEnabled();
      this.reload(cfs.metadata().params.compaction);
   }

   public AbstractCompactionTask getNextBackgroundTask(int gcBefore) {
      this.maybeReloadDiskBoundaries();
      this.readLock.lock();

      try {
         AbstractCompactionTask task;
         if(!this.isEnabled()) {
            task = null;
            return task;
         } else {
            task = this.getNextPendingRepairBackgroundTask();
            if(task != null) {
               AbstractCompactionTask var9 = task;
               return var9;
            } else {
               ArrayList<Pair<Integer, Supplier<AbstractCompactionTask>>> sortedSuppliers = new ArrayList(this.repaired.size() + this.unrepaired.size() + 1);
               Iterator suppliers = this.repaired.iterator();

               AbstractCompactionStrategy strategy;
               while(suppliers.hasNext()) {
                  strategy = (AbstractCompactionStrategy)suppliers.next();
                  sortedSuppliers.add(Pair.create(Integer.valueOf(strategy.getEstimatedRemainingTasks()), () -> {
                     return strategy.getNextBackgroundTask(gcBefore);
                  }));
               }

               suppliers = this.unrepaired.iterator();

               while(suppliers.hasNext()) {
                  strategy = (AbstractCompactionStrategy)suppliers.next();
                  sortedSuppliers.add(Pair.create(Integer.valueOf(strategy.getEstimatedRemainingTasks()), () -> {
                     return strategy.getNextBackgroundTask(gcBefore);
                  }));
               }

               suppliers = this.pendingRepairs.iterator();

               while(suppliers.hasNext()) {
                  PendingRepairManager pending = (PendingRepairManager)suppliers.next();
                  sortedSuppliers.add(Pair.create(Integer.valueOf(pending.getMaxEstimatedRemainingTasks()), () -> {
                     return pending.getNextBackgroundTask(gcBefore);
                  }));
               }

               sortedSuppliers.sort((x, y) -> {
                  return ((Integer)y.left).intValue() - ((Integer)x.left).intValue();
               });
               suppliers = Iterables.transform(sortedSuppliers, (p) -> {
                  return (Supplier)p.right;
               }).iterator();

               assert suppliers.hasNext();

               do {
                  task = (AbstractCompactionTask)((Supplier)suppliers.next()).get();
               } while(suppliers.hasNext() && task == null);

               AbstractCompactionTask var11 = task;
               return var11;
            }
         }
      } finally {
         this.readLock.unlock();
      }
   }

   public AbstractCompactionTask getNextPendingRepairBackgroundTask() {
      this.readLock.lock();

      try {
         ArrayList<Pair<Integer, PendingRepairManager>> pendingRepairManagers = new ArrayList(this.pendingRepairs.size());
         Iterator var2 = this.pendingRepairs.iterator();

         while(var2.hasNext()) {
            PendingRepairManager pendingRepair = (PendingRepairManager)var2.next();
            int numPending = pendingRepair.getNumPendingRepairFinishedTasks();
            if(numPending > 0) {
               pendingRepairManagers.add(Pair.create(Integer.valueOf(numPending), pendingRepair));
            }
         }

         if(pendingRepairManagers.isEmpty()) {
            return null;
         } else {
            pendingRepairManagers.sort((x, y) -> {
               return ((Integer)y.left).intValue() - ((Integer)x.left).intValue();
            });
            var2 = pendingRepairManagers.iterator();

            AbstractCompactionTask task;
            do {
               if(!var2.hasNext()) {
                  return null;
               }

               Pair<Integer, PendingRepairManager> pair = (Pair)var2.next();
               task = ((PendingRepairManager)pair.right).getNextRepairFinishedTask();
            } while(task == null);

            AbstractCompactionTask var5 = task;
            return var5;
         }
      } finally {
         this.readLock.unlock();
      }
   }

   public List<Runnable> getPendingRepairTasks(UUID sessionID) {
      this.readLock.lock();

      try {
         List<Runnable> tasks = new ArrayList(this.pendingRepairs.size());
         Iterator var3 = this.pendingRepairs.iterator();

         while(var3.hasNext()) {
            PendingRepairManager pendingRepair = (PendingRepairManager)var3.next();
            Runnable task = pendingRepair.getRepairFinishedTask(sessionID);
            if(task != null) {
               tasks.add(task);
            }
         }

         ArrayList var9 = tasks;
         return var9;
      } finally {
         this.readLock.unlock();
      }
   }

   public boolean isEnabled() {
      return this.enabled && this.isActive;
   }

   public boolean isActive() {
      return this.isActive;
   }

   public void resume() {
      this.writeLock.lock();

      try {
         this.isActive = true;
      } finally {
         this.writeLock.unlock();
      }

   }

   public void pause() {
      this.writeLock.lock();

      try {
         this.isActive = false;
      } finally {
         this.writeLock.unlock();
      }

   }

   private void startup() {
      this.writeLock.lock();

      try {
         Iterator var1 = this.cfs.getSSTables(SSTableSet.CANONICAL).iterator();

         while(var1.hasNext()) {
            SSTableReader sstable = (SSTableReader)var1.next();
            if(sstable.openReason != SSTableReader.OpenReason.EARLY) {
               this.compactionStrategyFor(sstable).addSSTable(sstable);
            }
         }

         this.repaired.forEach(AbstractCompactionStrategy::startup);
         this.unrepaired.forEach(AbstractCompactionStrategy::startup);
         this.pendingRepairs.forEach(PendingRepairManager::startup);
         this.shouldDefragment = ((AbstractCompactionStrategy)this.repaired.get(0)).shouldDefragment();
         this.supportsEarlyOpen = ((AbstractCompactionStrategy)this.repaired.get(0)).supportsEarlyOpen();
         this.fanout = this.repaired.get(0) instanceof LeveledCompactionStrategy?((LeveledCompactionStrategy)this.repaired.get(0)).getLevelFanoutSize():10;
      } finally {
         this.writeLock.unlock();
      }

      this.repaired.forEach(AbstractCompactionStrategy::startup);
      this.unrepaired.forEach(AbstractCompactionStrategy::startup);
      this.pendingRepairs.forEach(PendingRepairManager::startup);
      if(Stream.concat(this.repaired.stream(), this.unrepaired.stream()).anyMatch((cs) -> {
         return cs.logAll;
      })) {
         this.compactionLogger.enable();
      }

   }

   public AbstractCompactionStrategy getCompactionStrategyFor(SSTableReader sstable) {
      this.maybeReloadDiskBoundaries();
      return this.compactionStrategyFor(sstable);
   }

   @VisibleForTesting
   protected AbstractCompactionStrategy compactionStrategyFor(SSTableReader sstable) {
      this.readLock.lock();

      AbstractCompactionStrategy var3;
      try {
         int index = this.compactionStrategyIndexFor(sstable);
         if(sstable.isPendingRepair()) {
            var3 = ((PendingRepairManager)this.pendingRepairs.get(index)).getOrCreate(sstable);
            return var3;
         }

         if(sstable.isRepaired()) {
            var3 = (AbstractCompactionStrategy)this.repaired.get(index);
            return var3;
         }

         var3 = (AbstractCompactionStrategy)this.unrepaired.get(index);
      } finally {
         this.readLock.unlock();
      }

      return var3;
   }

   @VisibleForTesting
   protected int compactionStrategyIndexFor(SSTableReader sstable) {
      this.readLock.lock();

      byte var2;
      try {
         if(this.partitionSSTablesByTokenRange) {
            int var6 = this.currentBoundaries.getDiskIndex(sstable);
            return var6;
         }

         var2 = 0;
      } finally {
         this.readLock.unlock();
      }

      return var2;
   }

   @VisibleForTesting
   List<AbstractCompactionStrategy> getRepaired() {
      this.readLock.lock();

      ArrayList var1;
      try {
         var1 = Lists.newArrayList(this.repaired);
      } finally {
         this.readLock.unlock();
      }

      return var1;
   }

   @VisibleForTesting
   List<AbstractCompactionStrategy> getUnrepaired() {
      this.readLock.lock();

      ArrayList var1;
      try {
         var1 = Lists.newArrayList(this.unrepaired);
      } finally {
         this.readLock.unlock();
      }

      return var1;
   }

   @VisibleForTesting
   List<AbstractCompactionStrategy> getForPendingRepair(UUID sessionID) {
      this.readLock.lock();

      ArrayList var3;
      try {
         List<AbstractCompactionStrategy> strategies = new ArrayList(this.pendingRepairs.size());
         this.pendingRepairs.forEach((p) -> {
            strategies.add(p.get(sessionID));
         });
         var3 = strategies;
      } finally {
         this.readLock.unlock();
      }

      return var3;
   }

   @VisibleForTesting
   Set<UUID> pendingRepairs() {
      this.readLock.lock();

      HashSet var2;
      try {
         Set<UUID> ids = new HashSet();
         this.pendingRepairs.forEach((p) -> {
            ids.addAll(p.getSessions());
         });
         var2 = ids;
      } finally {
         this.readLock.unlock();
      }

      return var2;
   }

   public boolean hasDataForPendingRepair(UUID sessionID) {
      this.readLock.lock();

      boolean var2;
      try {
         var2 = Iterables.any(this.pendingRepairs, (prm) -> {
            return prm.hasDataForSession(sessionID);
         });
      } finally {
         this.readLock.unlock();
      }

      return var2;
   }

   public void shutdown() {
      this.writeLock.lock();

      try {
         this.isActive = false;
         this.repaired.forEach(AbstractCompactionStrategy::shutdown);
         this.unrepaired.forEach(AbstractCompactionStrategy::shutdown);
         this.pendingRepairs.forEach(PendingRepairManager::shutdown);
         this.compactionLogger.disable();
      } finally {
         this.writeLock.unlock();
      }

   }

   public void maybeReload(TableMetadata metadata) {
      if(!metadata.params.compaction.equals(this.schemaCompactionParams)) {
         this.writeLock.lock();

         try {
            if(metadata.params.compaction.equals(this.schemaCompactionParams)) {
               return;
            }

            this.reload(metadata.params.compaction);
         } finally {
            this.writeLock.unlock();
         }

      }
   }

   @VisibleForTesting
   protected boolean maybeReloadDiskBoundaries() {
      if(!this.currentBoundaries.isOutOfDate()) {
         return false;
      } else {
         this.writeLock.lock();

         boolean var1;
         try {
            if(!this.currentBoundaries.isOutOfDate()) {
               var1 = false;
               return var1;
            }

            this.reload(this.params);
            var1 = true;
         } finally {
            this.writeLock.unlock();
         }

         return var1;
      }
   }

   private void reload(CompactionParams newCompactionParams) {
      boolean enabledWithJMX = this.enabled && !this.shouldBeEnabled();
      boolean disabledWithJMX = !this.enabled && this.shouldBeEnabled();
      if(this.currentBoundaries != null) {
         if(!newCompactionParams.equals(this.schemaCompactionParams)) {
            logger.debug("Recreating compaction strategy - compaction parameters changed for {}.{}", this.cfs.keyspace.getName(), this.cfs.getTableName());
         } else if(this.currentBoundaries.isOutOfDate()) {
            logger.debug("Recreating compaction strategy - disk boundaries are out of date for {}.{}.", this.cfs.keyspace.getName(), this.cfs.getTableName());
         }
      }

      if(this.currentBoundaries == null || this.currentBoundaries.isOutOfDate()) {
         this.currentBoundaries = (DiskBoundaries)this.boundariesSupplier.get();
      }

      this.setStrategy(newCompactionParams);
      this.schemaCompactionParams = this.cfs.metadata().params.compaction;
      if(!disabledWithJMX && (this.shouldBeEnabled() || enabledWithJMX)) {
         this.enable();
      } else {
         this.disable();
      }

      this.startup();
   }

   public int getUnleveledSSTables() {
      this.maybeReloadDiskBoundaries();
      this.readLock.lock();

      int var9;
      try {
         if(!(this.repaired.get(0) instanceof LeveledCompactionStrategy) || !(this.unrepaired.get(0) instanceof LeveledCompactionStrategy)) {
            return 0;
         }

         int count = 0;

         Iterator var2;
         AbstractCompactionStrategy strategy;
         for(var2 = this.repaired.iterator(); var2.hasNext(); count += ((LeveledCompactionStrategy)strategy).getLevelSize(0)) {
            strategy = (AbstractCompactionStrategy)var2.next();
         }

         for(var2 = this.unrepaired.iterator(); var2.hasNext(); count += ((LeveledCompactionStrategy)strategy).getLevelSize(0)) {
            strategy = (AbstractCompactionStrategy)var2.next();
         }

         var2 = this.pendingRepairs.iterator();

         while(var2.hasNext()) {
            PendingRepairManager pendingManager = (PendingRepairManager)var2.next();

            AbstractCompactionStrategy strategy;
            for(Iterator var4 = pendingManager.getStrategies().iterator(); var4.hasNext(); count += ((LeveledCompactionStrategy)strategy).getLevelSize(0)) {
               strategy = (AbstractCompactionStrategy)var4.next();
            }
         }

         var9 = count;
      } finally {
         this.readLock.unlock();
      }

      return var9;
   }

   public int getLevelFanoutSize() {
      return this.fanout;
   }

   public int[] getSSTableCountPerLevel() {
      this.maybeReloadDiskBoundaries();
      this.readLock.lock();

      try {
         if(!(this.repaired.get(0) instanceof LeveledCompactionStrategy) || !(this.unrepaired.get(0) instanceof LeveledCompactionStrategy)) {
            return null;
         } else {
            int[] res = new int[LeveledManifest.MAX_LEVEL_COUNT];

            Iterator var2;
            AbstractCompactionStrategy strategy;
            int[] pendingRepairCountPerLevel;
            for(var2 = this.repaired.iterator(); var2.hasNext(); res = sumArrays(res, pendingRepairCountPerLevel)) {
               strategy = (AbstractCompactionStrategy)var2.next();
               pendingRepairCountPerLevel = ((LeveledCompactionStrategy)strategy).getAllLevelSize();
            }

            for(var2 = this.unrepaired.iterator(); var2.hasNext(); res = sumArrays(res, pendingRepairCountPerLevel)) {
               strategy = (AbstractCompactionStrategy)var2.next();
               pendingRepairCountPerLevel = ((LeveledCompactionStrategy)strategy).getAllLevelSize();
            }

            for(var2 = this.pendingRepairs.iterator(); var2.hasNext(); res = sumArrays(res, pendingRepairCountPerLevel)) {
               PendingRepairManager pending = (PendingRepairManager)var2.next();
               pendingRepairCountPerLevel = pending.getSSTableCountPerLevel();
            }

            int[] var8 = res;
            return var8;
         }
      } finally {
         this.readLock.unlock();
      }
   }

   static int[] sumArrays(int[] a, int[] b) {
      int[] res = new int[Math.max(a.length, b.length)];

      for(int i = 0; i < res.length; ++i) {
         if(i < a.length && i < b.length) {
            res[i] = a[i] + b[i];
         } else if(i < a.length) {
            res[i] = a[i];
         } else {
            res[i] = b[i];
         }
      }

      return res;
   }

   public boolean shouldDefragment() {
      return this.shouldDefragment;
   }

   public Directories getDirectories() {
      this.maybeReloadDiskBoundaries();
      this.readLock.lock();

      Directories var1;
      try {
         assert ((AbstractCompactionStrategy)this.repaired.get(0)).getClass().equals(((AbstractCompactionStrategy)this.unrepaired.get(0)).getClass());

         var1 = ((AbstractCompactionStrategy)this.repaired.get(0)).getDirectories();
      } finally {
         this.readLock.unlock();
      }

      return var1;
   }

   private void handleFlushNotification(Iterable<SSTableReader> added, boolean fromStream) {
      if(!this.maybeReloadDiskBoundaries()) {
         this.readLock.lock();

         try {
            Iterator var3 = added.iterator();

            while(var3.hasNext()) {
               SSTableReader sstable = (SSTableReader)var3.next();
               if(fromStream) {
                  this.compactionStrategyFor(sstable).addSSTableFromStreaming(sstable);
               } else {
                  this.compactionStrategyFor(sstable).addSSTable(sstable);
               }
            }
         } finally {
            this.readLock.unlock();
         }

      }
   }

   private void handleListChangedNotification(Iterable<SSTableReader> added, Iterable<SSTableReader> removed) {
      if(!this.maybeReloadDiskBoundaries()) {
         this.readLock.lock();

         try {
            Directories.DataDirectory[] locations = this.cfs.getDirectories().getWriteableLocations();
            int locationSize = this.cfs.getPartitioner().splitter().isPresent()?locations.length:1;
            List<Set<SSTableReader>> pendingRemoved = new ArrayList(locationSize);
            List<Set<SSTableReader>> pendingAdded = new ArrayList(locationSize);
            List<Set<SSTableReader>> repairedRemoved = new ArrayList(locationSize);
            List<Set<SSTableReader>> repairedAdded = new ArrayList(locationSize);
            List<Set<SSTableReader>> unrepairedRemoved = new ArrayList(locationSize);
            List<Set<SSTableReader>> unrepairedAdded = new ArrayList(locationSize);

            int i;
            for(i = 0; i < locationSize; ++i) {
               pendingRemoved.add(new HashSet());
               pendingAdded.add(new HashSet());
               repairedRemoved.add(new HashSet());
               repairedAdded.add(new HashSet());
               unrepairedRemoved.add(new HashSet());
               unrepairedAdded.add(new HashSet());
            }

            Iterator var17 = removed.iterator();

            SSTableReader sstable;
            int i;
            while(var17.hasNext()) {
               sstable = (SSTableReader)var17.next();
               i = this.compactionStrategyIndexFor(sstable);
               if(sstable.isPendingRepair()) {
                  ((Set)pendingRemoved.get(i)).add(sstable);
               } else if(sstable.isRepaired()) {
                  ((Set)repairedRemoved.get(i)).add(sstable);
               } else {
                  ((Set)unrepairedRemoved.get(i)).add(sstable);
               }
            }

            var17 = added.iterator();

            while(var17.hasNext()) {
               sstable = (SSTableReader)var17.next();
               i = this.compactionStrategyIndexFor(sstable);
               if(sstable.isPendingRepair()) {
                  ((Set)pendingAdded.get(i)).add(sstable);
               } else if(sstable.isRepaired()) {
                  ((Set)repairedAdded.get(i)).add(sstable);
               } else {
                  ((Set)unrepairedAdded.get(i)).add(sstable);
               }
            }

            for(i = 0; i < locationSize; ++i) {
               if(!((Set)pendingRemoved.get(i)).isEmpty()) {
                  ((PendingRepairManager)this.pendingRepairs.get(i)).replaceSSTables((Set)pendingRemoved.get(i), (Set)pendingAdded.get(i));
               } else {
                  PendingRepairManager pendingManager = (PendingRepairManager)this.pendingRepairs.get(i);
                  ((Set)pendingAdded.get(i)).forEach((s) -> {
                     pendingManager.addSSTable(s);
                  });
               }

               if(!((Set)repairedRemoved.get(i)).isEmpty()) {
                  ((AbstractCompactionStrategy)this.repaired.get(i)).replaceSSTables((Collection)repairedRemoved.get(i), (Collection)repairedAdded.get(i));
               } else {
                  ((AbstractCompactionStrategy)this.repaired.get(i)).addSSTables((Iterable)repairedAdded.get(i));
               }

               if(!((Set)unrepairedRemoved.get(i)).isEmpty()) {
                  ((AbstractCompactionStrategy)this.unrepaired.get(i)).replaceSSTables((Collection)unrepairedRemoved.get(i), (Collection)unrepairedAdded.get(i));
               } else {
                  ((AbstractCompactionStrategy)this.unrepaired.get(i)).addSSTables((Iterable)unrepairedAdded.get(i));
               }
            }
         } finally {
            this.readLock.unlock();
         }

      }
   }

   private void handleRepairStatusChangedNotification(Iterable<SSTableReader> sstables) {
      if(!this.maybeReloadDiskBoundaries()) {
         this.readLock.lock();

         try {
            Iterator var2 = sstables.iterator();

            while(var2.hasNext()) {
               SSTableReader sstable = (SSTableReader)var2.next();
               int index = this.compactionStrategyIndexFor(sstable);
               if(sstable.isPendingRepair()) {
                  ((PendingRepairManager)this.pendingRepairs.get(index)).addSSTable(sstable);
                  ((AbstractCompactionStrategy)this.unrepaired.get(index)).removeSSTable(sstable);
                  ((AbstractCompactionStrategy)this.repaired.get(index)).removeSSTable(sstable);
               } else if(sstable.isRepaired()) {
                  ((PendingRepairManager)this.pendingRepairs.get(index)).removeSSTable(sstable);
                  ((AbstractCompactionStrategy)this.unrepaired.get(index)).removeSSTable(sstable);
                  ((AbstractCompactionStrategy)this.repaired.get(index)).addSSTable(sstable);
               } else {
                  ((PendingRepairManager)this.pendingRepairs.get(index)).removeSSTable(sstable);
                  ((AbstractCompactionStrategy)this.repaired.get(index)).removeSSTable(sstable);
                  ((AbstractCompactionStrategy)this.unrepaired.get(index)).addSSTable(sstable);
               }
            }
         } finally {
            this.readLock.unlock();
         }

      }
   }

   private void handleDeletingNotification(SSTableReader deleted) {
      if(!this.maybeReloadDiskBoundaries()) {
         this.readLock.lock();

         try {
            this.compactionStrategyFor(deleted).removeSSTable(deleted);
         } finally {
            this.readLock.unlock();
         }

      }
   }

   public void handleNotification(INotification notification, Object sender) {
      if(notification instanceof SSTableAddedNotification) {
         this.handleFlushNotification(((SSTableAddedNotification)notification).added, ((SSTableAddedNotification)notification).fromStream);
      } else if(notification instanceof SSTableListChangedNotification) {
         SSTableListChangedNotification listChangedNotification = (SSTableListChangedNotification)notification;
         this.handleListChangedNotification(listChangedNotification.added, listChangedNotification.removed);
      } else if(notification instanceof SSTableRepairStatusChanged) {
         this.handleRepairStatusChangedNotification(((SSTableRepairStatusChanged)notification).sstables);
      } else if(notification instanceof SSTableDeletingNotification) {
         this.handleDeletingNotification(((SSTableDeletingNotification)notification).deleting);
      }

   }

   public void enable() {
      this.writeLock.lock();

      try {
         this.enabled = true;
      } finally {
         this.writeLock.unlock();
      }

   }

   public void disable() {
      this.writeLock.lock();

      try {
         this.enabled = false;
      } finally {
         this.writeLock.unlock();
      }

   }

   public AbstractCompactionStrategy.ScannerList maybeGetScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges) {
      this.maybeReloadDiskBoundaries();
      this.readLock.lock();
      ArrayList scanners = new ArrayList(sstables.size());

      try {
         assert this.repaired.size() == this.unrepaired.size();

         assert this.repaired.size() == this.pendingRepairs.size();

         int numRepaired = this.repaired.size();
         List<Set<SSTableReader>> pendingSSTables = new ArrayList(numRepaired);
         List<Set<SSTableReader>> repairedSSTables = new ArrayList(numRepaired);
         List<Set<SSTableReader>> unrepairedSSTables = new ArrayList(numRepaired);

         int i;
         for(i = 0; i < numRepaired; ++i) {
            pendingSSTables.add(new HashSet());
            repairedSSTables.add(new HashSet());
            unrepairedSSTables.add(new HashSet());
         }

         Iterator var16 = sstables.iterator();

         while(var16.hasNext()) {
            SSTableReader sstable = (SSTableReader)var16.next();
            int idx = this.compactionStrategyIndexFor(sstable);
            if(sstable.isPendingRepair()) {
               ((Set)pendingSSTables.get(idx)).add(sstable);
            } else if(sstable.isRepaired()) {
               ((Set)repairedSSTables.get(idx)).add(sstable);
            } else {
               ((Set)unrepairedSSTables.get(idx)).add(sstable);
            }
         }

         for(i = 0; i < pendingSSTables.size(); ++i) {
            if(!((Set)pendingSSTables.get(i)).isEmpty()) {
               scanners.addAll(((PendingRepairManager)this.pendingRepairs.get(i)).getScanners((Collection)pendingSSTables.get(i), ranges));
            }
         }

         for(i = 0; i < repairedSSTables.size(); ++i) {
            if(!((Set)repairedSSTables.get(i)).isEmpty()) {
               scanners.addAll(((AbstractCompactionStrategy)this.repaired.get(i)).getScanners((Collection)repairedSSTables.get(i), ranges).scanners);
            }
         }

         for(i = 0; i < unrepairedSSTables.size(); ++i) {
            if(!((Set)unrepairedSSTables.get(i)).isEmpty()) {
               scanners.addAll(((AbstractCompactionStrategy)this.unrepaired.get(i)).getScanners((Collection)unrepairedSSTables.get(i), ranges).scanners);
            }
         }
      } catch (PendingRepairManager.IllegalSSTableArgumentException var14) {
         ISSTableScanner.closeAllAndPropagate(scanners, new ConcurrentModificationException(var14));
      } finally {
         this.readLock.unlock();
      }

      return new AbstractCompactionStrategy.ScannerList(scanners);
   }

   public AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges) {
      while(true) {
         try {
            return this.maybeGetScanners(sstables, ranges);
         } catch (ConcurrentModificationException var4) {
            logger.debug("SSTable repairedAt/pendingRepaired values changed while getting scanners");
         }
      }
   }

   public AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> sstables) {
      return this.getScanners(sstables, (Collection)null);
   }

   public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> sstablesToGroup) {
      this.maybeReloadDiskBoundaries();
      this.readLock.lock();

      try {
         Map<Integer, List<SSTableReader>> groups = (Map)sstablesToGroup.stream().collect(Collectors.groupingBy((s) -> {
            return Integer.valueOf(this.compactionStrategyIndexFor(s));
         }));
         Collection<Collection<SSTableReader>> anticompactionGroups = new ArrayList();
         Iterator var4 = groups.entrySet().iterator();

         while(var4.hasNext()) {
            Entry<Integer, List<SSTableReader>> group = (Entry)var4.next();
            anticompactionGroups.addAll(((AbstractCompactionStrategy)this.unrepaired.get(((Integer)group.getKey()).intValue())).groupSSTablesForAntiCompaction((Collection)group.getValue()));
         }

         ArrayList var9 = anticompactionGroups;
         return var9;
      } finally {
         this.readLock.unlock();
      }
   }

   public long getMaxSSTableBytes() {
      this.readLock.lock();

      long var1;
      try {
         var1 = ((AbstractCompactionStrategy)this.unrepaired.get(0)).getMaxSSTableBytes();
      } finally {
         this.readLock.unlock();
      }

      return var1;
   }

   public AbstractCompactionTask getCompactionTask(LifecycleTransaction txn, int gcBefore, long maxSSTableBytes) {
      this.maybeReloadDiskBoundaries();
      this.readLock.lock();

      AbstractCompactionTask var5;
      try {
         this.validateForCompaction(txn.originals());
         var5 = this.compactionStrategyFor((SSTableReader)txn.originals().iterator().next()).getCompactionTask(txn, gcBefore, maxSSTableBytes);
      } finally {
         this.readLock.unlock();
      }

      return var5;
   }

   private void validateForCompaction(Iterable<SSTableReader> input) {
      this.readLock.lock();

      try {
         SSTableReader firstSSTable = (SSTableReader)Iterables.getFirst(input, (Object)null);

         assert firstSSTable != null;

         boolean repaired = firstSSTable.isRepaired();
         int firstIndex = this.compactionStrategyIndexFor(firstSSTable);
         boolean isPending = firstSSTable.isPendingRepair();
         UUID pendingRepair = firstSSTable.getSSTableMetadata().pendingRepair;
         Iterator var7 = input.iterator();

         while(var7.hasNext()) {
            SSTableReader sstable = (SSTableReader)var7.next();
            if(sstable.isRepaired() != repaired) {
               throw new UnsupportedOperationException("You can't mix repaired and unrepaired data in a compaction");
            }

            if(firstIndex != this.compactionStrategyIndexFor(sstable)) {
               throw new UnsupportedOperationException("You can't mix sstables from different directories in a compaction");
            }

            if(isPending && !pendingRepair.equals(sstable.getSSTableMetadata().pendingRepair)) {
               throw new UnsupportedOperationException("You can't compact sstables from different pending repair sessions");
            }
         }
      } finally {
         this.readLock.unlock();
      }

   }

   public Collection<AbstractCompactionTask> getMaximalTasks(final int gcBefore, final boolean splitOutput) {
      this.maybeReloadDiskBoundaries();
      return (Collection)this.cfs.runWithCompactionsDisabled(new Callable<Collection<AbstractCompactionTask>>() {
         public Collection<AbstractCompactionTask> call() {
            List<AbstractCompactionTask> tasks = new ArrayList();
            CompactionStrategyManager.this.readLock.lock();

            try {
               Iterator var2 = CompactionStrategyManager.this.repaired.iterator();

               AbstractCompactionStrategy strategy;
               Collection pendingRepairTasks;
               while(var2.hasNext()) {
                  strategy = (AbstractCompactionStrategy)var2.next();
                  pendingRepairTasks = strategy.getMaximalTask(gcBefore, splitOutput);
                  if(pendingRepairTasks != null) {
                     tasks.addAll(pendingRepairTasks);
                  }
               }

               var2 = CompactionStrategyManager.this.unrepaired.iterator();

               while(var2.hasNext()) {
                  strategy = (AbstractCompactionStrategy)var2.next();
                  pendingRepairTasks = strategy.getMaximalTask(gcBefore, splitOutput);
                  if(pendingRepairTasks != null) {
                     tasks.addAll(pendingRepairTasks);
                  }
               }

               var2 = CompactionStrategyManager.this.pendingRepairs.iterator();

               while(var2.hasNext()) {
                  PendingRepairManager pending = (PendingRepairManager)var2.next();
                  pendingRepairTasks = pending.getMaximalTasks(gcBefore, splitOutput);
                  if(pendingRepairTasks != null) {
                     tasks.addAll(pendingRepairTasks);
                  }
               }
            } finally {
               CompactionStrategyManager.this.readLock.unlock();
            }

            return tasks.isEmpty()?null:tasks;
         }
      }, false, false);
   }

   public List<AbstractCompactionTask> getUserDefinedTasks(Collection<SSTableReader> sstables, int gcBefore) {
      this.maybeReloadDiskBoundaries();
      List<AbstractCompactionTask> ret = new ArrayList();
      this.readLock.lock();

      try {
         Map<Integer, List<SSTableReader>> repairedSSTables = (Map)sstables.stream().filter((s) -> {
            return !s.isMarkedSuspect() && s.isRepaired() && !s.isPendingRepair();
         }).collect(Collectors.groupingBy((s) -> {
            return Integer.valueOf(this.compactionStrategyIndexFor(s));
         }));
         Map<Integer, List<SSTableReader>> unrepairedSSTables = (Map)sstables.stream().filter((s) -> {
            return !s.isMarkedSuspect() && !s.isRepaired() && !s.isPendingRepair();
         }).collect(Collectors.groupingBy((s) -> {
            return Integer.valueOf(this.compactionStrategyIndexFor(s));
         }));
         Map<Integer, List<SSTableReader>> pendingSSTables = (Map)sstables.stream().filter((s) -> {
            return !s.isMarkedSuspect() && s.isPendingRepair();
         }).collect(Collectors.groupingBy((s) -> {
            return Integer.valueOf(this.compactionStrategyIndexFor(s));
         }));
         Iterator var7 = repairedSSTables.entrySet().iterator();

         Entry group;
         while(var7.hasNext()) {
            group = (Entry)var7.next();
            ret.add(((AbstractCompactionStrategy)this.repaired.get(((Integer)group.getKey()).intValue())).getUserDefinedTask((Collection)group.getValue(), gcBefore));
         }

         var7 = unrepairedSSTables.entrySet().iterator();

         while(var7.hasNext()) {
            group = (Entry)var7.next();
            ret.add(((AbstractCompactionStrategy)this.unrepaired.get(((Integer)group.getKey()).intValue())).getUserDefinedTask((Collection)group.getValue(), gcBefore));
         }

         var7 = pendingSSTables.entrySet().iterator();

         while(var7.hasNext()) {
            group = (Entry)var7.next();
            ret.addAll(((PendingRepairManager)this.pendingRepairs.get(((Integer)group.getKey()).intValue())).createUserDefinedTasks((List)group.getValue(), gcBefore));
         }

         ArrayList var12 = ret;
         return var12;
      } finally {
         this.readLock.unlock();
      }
   }

   public int getEstimatedRemainingTasks() {
      this.maybeReloadDiskBoundaries();
      int tasks = 0;
      this.readLock.lock();

      try {
         Iterator var2;
         AbstractCompactionStrategy strategy;
         for(var2 = this.repaired.iterator(); var2.hasNext(); tasks += strategy.getEstimatedRemainingTasks()) {
            strategy = (AbstractCompactionStrategy)var2.next();
         }

         for(var2 = this.unrepaired.iterator(); var2.hasNext(); tasks += strategy.getEstimatedRemainingTasks()) {
            strategy = (AbstractCompactionStrategy)var2.next();
         }

         PendingRepairManager pending;
         for(var2 = this.pendingRepairs.iterator(); var2.hasNext(); tasks += pending.getEstimatedRemainingTasks()) {
            pending = (PendingRepairManager)var2.next();
         }
      } finally {
         this.readLock.unlock();
      }

      return tasks;
   }

   public boolean shouldBeEnabled() {
      return this.params.isEnabled();
   }

   public String getName() {
      this.maybeReloadDiskBoundaries();
      this.readLock.lock();

      String var1;
      try {
         var1 = ((AbstractCompactionStrategy)this.unrepaired.get(0)).getName();
      } finally {
         this.readLock.unlock();
      }

      return var1;
   }

   public List<List<AbstractCompactionStrategy>> getStrategies() {
      this.maybeReloadDiskBoundaries();
      this.readLock.lock();

      List var2;
      try {
         List<AbstractCompactionStrategy> pending = new ArrayList();
         this.pendingRepairs.forEach((p) -> {
            pending.addAll(p.getStrategies());
         });
         var2 = Arrays.asList(new List[]{this.repaired, this.unrepaired, pending});
      } finally {
         this.readLock.unlock();
      }

      return var2;
   }

   public void setNewLocalCompactionStrategy(CompactionParams params) {
      logger.info("Switching local compaction strategy from {} to {}}", this.params, params);
      this.writeLock.lock();

      try {
         this.setStrategy(params);
         if(this.shouldBeEnabled()) {
            this.enable();
         } else {
            this.disable();
         }

         this.startup();
      } finally {
         this.writeLock.unlock();
      }

   }

   private void setStrategy(CompactionParams params) {
      this.repaired.forEach(AbstractCompactionStrategy::shutdown);
      this.unrepaired.forEach(AbstractCompactionStrategy::shutdown);
      this.pendingRepairs.forEach(PendingRepairManager::shutdown);
      this.repaired.clear();
      this.unrepaired.clear();
      this.pendingRepairs.clear();
      if(this.partitionSSTablesByTokenRange) {
         for(int i = 0; i < this.currentBoundaries.directories.size(); ++i) {
            this.repaired.add(this.cfs.createCompactionStrategyInstance(params));
            this.unrepaired.add(this.cfs.createCompactionStrategyInstance(params));
            this.pendingRepairs.add(new PendingRepairManager(this.cfs, params));
         }
      } else {
         this.repaired.add(this.cfs.createCompactionStrategyInstance(params));
         this.unrepaired.add(this.cfs.createCompactionStrategyInstance(params));
         this.pendingRepairs.add(new PendingRepairManager(this.cfs, params));
      }

      this.params = params;
   }

   public CompactionParams getCompactionParams() {
      return this.params;
   }

   public boolean onlyPurgeRepairedTombstones() {
      return Boolean.parseBoolean((String)this.params.options().get("only_purge_repaired_tombstones"));
   }

   public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, MetadataCollector collector, SerializationHeader header, Collection<Index> indexes, LifecycleTransaction txn) {
      this.maybeReloadDiskBoundaries();
      this.readLock.lock();

      SSTableMultiWriter var12;
      try {
         int index = this.partitionSSTablesByTokenRange?this.currentBoundaries.getBoundariesFromSSTableDirectory(descriptor):0;
         if(pendingRepair != ActiveRepairService.NO_PENDING_REPAIR) {
            var12 = ((PendingRepairManager)this.pendingRepairs.get(index)).getOrCreate(pendingRepair).createSSTableMultiWriter(descriptor, keyCount, 0L, pendingRepair, collector, header, indexes, txn);
            return var12;
         }

         if(repairedAt == 0L) {
            var12 = ((AbstractCompactionStrategy)this.unrepaired.get(index)).createSSTableMultiWriter(descriptor, keyCount, repairedAt, ActiveRepairService.NO_PENDING_REPAIR, collector, header, indexes, txn);
            return var12;
         }

         var12 = ((AbstractCompactionStrategy)this.repaired.get(index)).createSSTableMultiWriter(descriptor, keyCount, repairedAt, ActiveRepairService.NO_PENDING_REPAIR, collector, header, indexes, txn);
      } finally {
         this.readLock.unlock();
      }

      return var12;
   }

   public boolean isRepaired(AbstractCompactionStrategy strategy) {
      return this.repaired.contains(strategy);
   }

   public List<String> getStrategyFolders(AbstractCompactionStrategy strategy) {
      this.readLock.lock();

      try {
         Directories.DataDirectory[] locations = this.cfs.getDirectories().getWriteableLocations();
         int i;
         if(this.partitionSSTablesByTokenRange) {
            int unrepairedIndex = this.unrepaired.indexOf(strategy);
            if(unrepairedIndex > 0) {
               List var14 = Collections.singletonList(locations[unrepairedIndex].location.getAbsolutePath());
               return var14;
            }

            int repairedIndex = this.repaired.indexOf(strategy);
            if(repairedIndex > 0) {
               List var15 = Collections.singletonList(locations[repairedIndex].location.getAbsolutePath());
               return var15;
            }

            for(i = 0; i < this.pendingRepairs.size(); ++i) {
               PendingRepairManager pending = (PendingRepairManager)this.pendingRepairs.get(i);
               if(pending.hasStrategy(strategy)) {
                  List var7 = Collections.singletonList(locations[i].location.getAbsolutePath());
                  return var7;
               }
            }
         }

         List<String> folders = new ArrayList(locations.length);
         Directories.DataDirectory[] var12 = locations;
         i = locations.length;

         for(int var16 = 0; var16 < i; ++var16) {
            Directories.DataDirectory location = var12[var16];
            folders.add(location.location.getAbsolutePath());
         }

         ArrayList var13 = folders;
         return var13;
      } finally {
         this.readLock.unlock();
      }
   }

   public boolean supportsEarlyOpen() {
      return this.supportsEarlyOpen;
   }

   @VisibleForTesting
   List<PendingRepairManager> getPendingRepairManagers() {
      this.maybeReloadDiskBoundaries();
      this.readLock.lock();

      List var1;
      try {
         var1 = this.pendingRepairs;
      } finally {
         this.readLock.unlock();
      }

      return var1;
   }

   public int mutateRepaired(Collection<SSTableReader> sstables, long repairedAt, UUID pendingRepair) throws IOException {
      Set<SSTableReader> changed = new HashSet();
      this.writeLock.lock();

      try {
         Iterator var6 = sstables.iterator();

         while(var6.hasNext()) {
            SSTableReader sstable = (SSTableReader)var6.next();
            sstable.descriptor.getMetadataSerializer().mutateRepaired(sstable.descriptor, repairedAt, pendingRepair);
            sstable.reloadSSTableMetadata();
            changed.add(sstable);
         }
      } finally {
         try {
            this.cfs.getTracker().notifySSTableRepairedStatusChanged(changed);
         } finally {
            this.writeLock.unlock();
         }
      }

      return changed.size();
   }
}
