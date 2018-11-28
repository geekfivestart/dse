package org.apache.cassandra.db.compaction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.collect.ImmutableMap.Builder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PendingRepairManager {
   private static final Logger logger = LoggerFactory.getLogger(PendingRepairManager.class);
   private final ColumnFamilyStore cfs;
   private final CompactionParams params;
   private volatile ImmutableMap<UUID, AbstractCompactionStrategy> strategies = ImmutableMap.of();

   PendingRepairManager(ColumnFamilyStore cfs, CompactionParams params) {
      this.cfs = cfs;
      this.params = params;
   }

   private Builder<UUID, AbstractCompactionStrategy> mapBuilder() {
      return ImmutableMap.builder();
   }

   AbstractCompactionStrategy get(UUID id) {
      return (AbstractCompactionStrategy)this.strategies.get(id);
   }

   AbstractCompactionStrategy get(SSTableReader sstable) {
      assert sstable.isPendingRepair();

      return this.get(sstable.getSSTableMetadata().pendingRepair);
   }

   AbstractCompactionStrategy getOrCreate(UUID id) {
      checkPendingID(id);

      assert id != null;

      AbstractCompactionStrategy strategy = this.get(id);
      if(strategy == null) {
         synchronized(this) {
            strategy = this.get(id);
            if(strategy == null) {
               logger.debug("Creating {}.{} compaction strategy for pending repair: {}", new Object[]{this.cfs.metadata.keyspace, this.cfs.metadata.name, id});
               strategy = this.cfs.createCompactionStrategyInstance(this.params);
               this.strategies = this.mapBuilder().putAll(this.strategies).put(id, strategy).build();
            }
         }
      }

      return strategy;
   }

   private static void checkPendingID(UUID pendingID) {
      if(pendingID == null) {
         throw new PendingRepairManager.IllegalSSTableArgumentException("sstable is not pending repair");
      }
   }

   AbstractCompactionStrategy getOrCreate(SSTableReader sstable) {
      return this.getOrCreate(sstable.getSSTableMetadata().pendingRepair);
   }

   private synchronized void removeSession(UUID sessionID) {
      if(this.strategies.containsKey(sessionID) && ((AbstractCompactionStrategy)this.strategies.get(sessionID)).getSSTables().isEmpty()) {
         logger.debug("Removing compaction strategy for pending repair {} on {}.{}", new Object[]{sessionID, this.cfs.metadata.keyspace, this.cfs.metadata.name});
         this.strategies = ImmutableMap.copyOf(Maps.filterKeys(this.strategies, (k) -> {
            return !k.equals(sessionID);
         }));
      }
   }

   synchronized void removeSSTable(SSTableReader sstable) {
      UnmodifiableIterator var2 = this.strategies.values().iterator();

      while(var2.hasNext()) {
         AbstractCompactionStrategy strategy = (AbstractCompactionStrategy)var2.next();
         strategy.removeSSTable(sstable);
      }

   }

   synchronized void addSSTable(SSTableReader sstable) {
      this.getOrCreate(sstable).addSSTable(sstable);
   }

   synchronized void replaceSSTables(Set<SSTableReader> removed, Set<SSTableReader> added) {
      if(!removed.isEmpty() || !added.isEmpty()) {
         Map<UUID, Pair<Set<SSTableReader>, Set<SSTableReader>>> groups = new HashMap();

         Iterator var4;
         SSTableReader sstable;
         UUID sessionID;
         for(var4 = removed.iterator(); var4.hasNext(); ((Set)((Pair)groups.get(sessionID)).left).add(sstable)) {
            sstable = (SSTableReader)var4.next();
            sessionID = sstable.getSSTableMetadata().pendingRepair;
            if(!groups.containsKey(sessionID)) {
               groups.put(sessionID, Pair.create(new HashSet(), new HashSet()));
            }
         }

         for(var4 = added.iterator(); var4.hasNext(); ((Set)((Pair)groups.get(sessionID)).right).add(sstable)) {
            sstable = (SSTableReader)var4.next();
            sessionID = sstable.getSSTableMetadata().pendingRepair;
            if(!groups.containsKey(sessionID)) {
               groups.put(sessionID, Pair.create(new HashSet(), new HashSet()));
            }
         }

         var4 = groups.entrySet().iterator();

         while(var4.hasNext()) {
            Entry<UUID, Pair<Set<SSTableReader>, Set<SSTableReader>>> entry = (Entry)var4.next();
            AbstractCompactionStrategy strategy = this.getOrCreate((UUID)entry.getKey());
            Set<SSTableReader> groupRemoved = (Set)((Pair)entry.getValue()).left;
            Set<SSTableReader> groupAdded = (Set)((Pair)entry.getValue()).right;
            if(!groupRemoved.isEmpty()) {
               strategy.replaceSSTables(groupRemoved, groupAdded);
            } else {
               strategy.addSSTables(groupAdded);
            }
         }

      }
   }

   synchronized void startup() {
      this.strategies.values().forEach(AbstractCompactionStrategy::startup);
   }

   synchronized void shutdown() {
      this.strategies.values().forEach(AbstractCompactionStrategy::shutdown);
   }

   private int getEstimatedRemainingTasks(UUID sessionID, AbstractCompactionStrategy strategy) {
      return this.canCleanup(sessionID)?0:strategy.getEstimatedRemainingTasks();
   }

   int getEstimatedRemainingTasks() {
      int tasks = 0;

      Entry entry;
      for(UnmodifiableIterator var2 = this.strategies.entrySet().iterator(); var2.hasNext(); tasks += this.getEstimatedRemainingTasks((UUID)entry.getKey(), (AbstractCompactionStrategy)entry.getValue())) {
         entry = (Entry)var2.next();
      }

      return tasks;
   }

   int getMaxEstimatedRemainingTasks() {
      int tasks = 0;

      Entry entry;
      for(UnmodifiableIterator var2 = this.strategies.entrySet().iterator(); var2.hasNext(); tasks = Math.max(tasks, this.getEstimatedRemainingTasks((UUID)entry.getKey(), (AbstractCompactionStrategy)entry.getValue()))) {
         entry = (Entry)var2.next();
      }

      return tasks;
   }

   private PendingRepairManager.RepairFinishedCompactionTask getRepairFinishedCompactionTask(UUID sessionID) {
      Set<SSTableReader> sstables = this.get(sessionID).getSSTables();
      long repairedAt = ActiveRepairService.instance.consistent.local.getFinalSessionRepairedAt(sessionID);
      LifecycleTransaction txn = this.cfs.getTracker().tryModify((Iterable)sstables, OperationType.COMPACTION);
      return txn == null?null:new PendingRepairManager.RepairFinishedCompactionTask(this.cfs, txn, sessionID, repairedAt);
   }

   synchronized Runnable getRepairFinishedTask(UUID sessionID) {
      if(this.canCleanup(sessionID) && this.get(sessionID) != null) {
         Set<SSTableReader> sstables = this.get(sessionID).getSSTables();
         long repairedAt = ActiveRepairService.instance.consistent.local.getFinalSessionRepairedAt(sessionID);
         return new PendingRepairManager.RepairFinishedTask(this.cfs, sstables, sessionID, repairedAt);
      } else {
         return null;
      }
   }

   synchronized int getNumPendingRepairFinishedTasks() {
      int count = 0;
      UnmodifiableIterator var2 = this.strategies.keySet().iterator();

      while(var2.hasNext()) {
         UUID sessionID = (UUID)var2.next();
         if(this.canCleanup(sessionID)) {
            ++count;
         }
      }

      return count;
   }

   synchronized AbstractCompactionTask getNextRepairFinishedTask() {
      UnmodifiableIterator var1 = this.strategies.keySet().iterator();

      UUID sessionID;
      do {
         if(!var1.hasNext()) {
            return null;
         }

         sessionID = (UUID)var1.next();
      } while(!this.canCleanup(sessionID));

      return this.getRepairFinishedCompactionTask(sessionID);
   }

   synchronized AbstractCompactionTask getNextBackgroundTask(int gcBefore) {
      if(this.strategies.isEmpty()) {
         return null;
      } else {
         Map<UUID, Integer> numTasks = new HashMap(this.strategies.size());
         ArrayList<UUID> sessions = new ArrayList(this.strategies.size());
         UnmodifiableIterator var4 = this.strategies.entrySet().iterator();

         while(var4.hasNext()) {
            Entry<UUID, AbstractCompactionStrategy> entry = (Entry)var4.next();
            if(!this.canCleanup((UUID)entry.getKey())) {
               numTasks.put(entry.getKey(), Integer.valueOf(this.getEstimatedRemainingTasks((UUID)entry.getKey(), (AbstractCompactionStrategy)entry.getValue())));
               sessions.add(entry.getKey());
            }
         }

         if(sessions.isEmpty()) {
            return null;
         } else {
            sessions.sort((o1, o2) -> {
               return ((Integer)numTasks.get(o2)).intValue() - ((Integer)numTasks.get(o1)).intValue();
            });
            UUID sessionID = (UUID)sessions.get(0);
            return this.get(sessionID).getNextBackgroundTask(gcBefore);
         }
      }
   }

   synchronized Collection<AbstractCompactionTask> getMaximalTasks(int gcBefore, boolean splitOutput) {
      if(this.strategies.isEmpty()) {
         return null;
      } else {
         List<AbstractCompactionTask> maximalTasks = new ArrayList(this.strategies.size());
         UnmodifiableIterator var4 = this.strategies.entrySet().iterator();

         while(var4.hasNext()) {
            Entry<UUID, AbstractCompactionStrategy> entry = (Entry)var4.next();
            if(this.canCleanup((UUID)entry.getKey())) {
               maximalTasks.add(this.getRepairFinishedCompactionTask((UUID)entry.getKey()));
            } else {
               Collection<AbstractCompactionTask> tasks = ((AbstractCompactionStrategy)entry.getValue()).getMaximalTask(gcBefore, splitOutput);
               if(tasks != null) {
                  maximalTasks.addAll(tasks);
               }
            }
         }

         return !maximalTasks.isEmpty()?maximalTasks:null;
      }
   }

   Collection<AbstractCompactionStrategy> getStrategies() {
      return this.strategies.values();
   }

   Set<UUID> getSessions() {
      return this.strategies.keySet();
   }

   boolean canCleanup(UUID sessionID) {
      return !ActiveRepairService.instance.consistent.local.isSessionInProgress(sessionID);
   }

   synchronized int[] getSSTableCountPerLevel() {
      int[] res = new int[LeveledManifest.MAX_LEVEL_COUNT];

      int[] counts;
      for(UnmodifiableIterator var2 = this.strategies.values().iterator(); var2.hasNext(); res = CompactionStrategyManager.sumArrays(res, counts)) {
         AbstractCompactionStrategy strategy = (AbstractCompactionStrategy)var2.next();

         assert strategy instanceof LeveledCompactionStrategy;

         counts = ((LeveledCompactionStrategy)strategy).getAllLevelSize();
      }

      return res;
   }

   synchronized Set<ISSTableScanner> getScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges) {
      if(sstables.isEmpty()) {
         return Collections.emptySet();
      } else {
         Map<UUID, Set<SSTableReader>> sessionSSTables = new HashMap();
         Iterator var4 = sstables.iterator();

         while(var4.hasNext()) {
            SSTableReader sstable = (SSTableReader)var4.next();
            UUID sessionID = sstable.getSSTableMetadata().pendingRepair;
            checkPendingID(sessionID);
            ((Set)sessionSSTables.computeIfAbsent(sessionID, (k) -> {
               return new HashSet();
            })).add(sstable);
         }

         HashSet scanners = new HashSet(sessionSSTables.size());

         try {
            Iterator var9 = sessionSSTables.entrySet().iterator();

            while(var9.hasNext()) {
               Entry<UUID, Set<SSTableReader>> entry = (Entry)var9.next();
               scanners.addAll(this.getOrCreate((UUID)entry.getKey()).getScanners((Collection)entry.getValue(), ranges).scanners);
            }
         } catch (Throwable var7) {
            ISSTableScanner.closeAllAndPropagate(scanners, var7);
         }

         return scanners;
      }
   }

   public boolean hasStrategy(AbstractCompactionStrategy strategy) {
      return this.strategies.values().contains(strategy);
   }

   public synchronized boolean hasDataForSession(UUID sessionID) {
      return this.strategies.keySet().contains(sessionID);
   }

   public Collection<AbstractCompactionTask> createUserDefinedTasks(List<SSTableReader> sstables, int gcBefore) {
      Map<UUID, List<SSTableReader>> group = (Map)sstables.stream().collect(Collectors.groupingBy((s) -> {
         return s.getSSTableMetadata().pendingRepair;
      }));
      return (Collection)group.entrySet().stream().map((g) -> {
         return ((AbstractCompactionStrategy)this.strategies.get(g.getKey())).getUserDefinedTask((Collection)g.getValue(), gcBefore);
      }).collect(Collectors.toList());
   }

   class RepairFinishedCompactionTask extends AbstractCompactionTask {
      private final UUID sessionID;
      private final long repairedAt;

      RepairFinishedCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction transaction, UUID sessionID, long repairedAt) {
         super(cfs, transaction);
         this.sessionID = sessionID;
         this.repairedAt = repairedAt;
      }

      @VisibleForTesting
      UUID getSessionID() {
         return this.sessionID;
      }

      protected void runMayThrow() throws Exception {
         try {
            (PendingRepairManager.this.new RepairFinishedTask(this.cfs, this.transaction.originals(), this.sessionID, this.repairedAt)).run();
         } finally {
            this.transaction.abort();
         }

      }

      public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables) {
         throw new UnsupportedOperationException();
      }

      protected int executeInternal(CompactionManager.CompactionExecutorStatsCollector collector) {
         this.run();
         return this.transaction.originals().size();
      }
   }

   class RepairFinishedTask implements Runnable {
      final ColumnFamilyStore cfs;
      final Collection<SSTableReader> sstables;
      final UUID sessionID;
      final long repairedAt;

      public RepairFinishedTask(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, UUID sessionID, long repairedAt) {
         this.cfs = cfs;
         this.sstables = sstables;
         this.sessionID = sessionID;
         this.repairedAt = repairedAt;
      }

      public void run() {
         boolean completed = false;

         try {
            this.cfs.getCompactionStrategyManager().mutateRepaired(this.sstables, this.repairedAt, ActiveRepairService.NO_PENDING_REPAIR);
            completed = true;
         } catch (IOException var6) {
            PendingRepairManager.logger.warn(var6.getMessage(), var6);
         } finally {
            this.cfs.getTracker().notifySSTableRepairedStatusChanged(this.sstables);
            if(completed) {
               PendingRepairManager.this.removeSession(this.sessionID);
            }

         }

      }
   }

   public static class IllegalSSTableArgumentException extends IllegalArgumentException {
      public IllegalSSTableArgumentException(String s) {
         super(s);
      }
   }
}
