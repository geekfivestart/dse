package org.apache.cassandra.db.view;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Striped;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.net.DroppingResponseException;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.schema.Views;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.ExecutableLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ViewManager {
   private static final Logger logger = LoggerFactory.getLogger(ViewManager.class);
   private static final int LOCK_STRIPES = Integer.getInteger("cassandra.view.lock_stripes", TPC.getNumCores() * 1024 * 4).intValue();
   private static final Striped<Semaphore> SEMAPHORES;
   private static final ConcurrentMap<Semaphore, Pair<Long, ExecutableLock>> LOCKS;
   private static final AtomicLong LOCK_ID_GEN;
   private static final boolean enableCoordinatorBatchlog;
   private final ConcurrentMap<String, View> viewsByName = new ConcurrentHashMap();
   private final ConcurrentMap<TableId, TableViews> viewsByBaseTable = new ConcurrentHashMap();
   private final Keyspace keyspace;

   public ViewManager(Keyspace keyspace) {
      this.keyspace = keyspace;
   }

   public boolean updatesAffectView(Collection<? extends IMutation> mutations, boolean coordinatorBatchlog) {
      if(!enableCoordinatorBatchlog && coordinatorBatchlog) {
         return false;
      } else {
         Iterator var3 = mutations.iterator();

         label42:
         while(var3.hasNext()) {
            IMutation mutation = (IMutation)var3.next();
            Iterator var5 = mutation.getPartitionUpdates().iterator();

            PartitionUpdate update;
            do {
               do {
                  if(!var5.hasNext()) {
                     continue label42;
                  }

                  update = (PartitionUpdate)var5.next();

                  assert this.keyspace.getName().equals(update.metadata().keyspace);
               } while(coordinatorBatchlog && this.keyspace.getReplicationStrategy().getReplicationFactor() == 1);
            } while(this.forTable(update.metadata().id).updatedViews(update).isEmpty());

            return true;
         }

         return false;
      }
   }

   private Iterable<View> allViews() {
      return this.viewsByName.values();
   }

   public void reload(boolean buildAllViews) {
      Views views = this.keyspace.getMetadata().views;
      Map<String, ViewMetadata> newViewsByName = Maps.newHashMapWithExpectedSize(views.size());
      Iterator var4 = views.iterator();

      while(var4.hasNext()) {
         ViewMetadata definition = (ViewMetadata)var4.next();
         newViewsByName.put(definition.name, definition);
      }

      var4 = newViewsByName.entrySet().iterator();

      while(var4.hasNext()) {
         Entry<String, ViewMetadata> entry = (Entry)var4.next();
         if(!this.viewsByName.containsKey(entry.getKey())) {
            this.addView((ViewMetadata)entry.getValue());
         }
      }

      if(buildAllViews) {
         if(!StorageService.instance.isInitialized()) {
            logger.info("Not submitting build tasks for views in keyspace {} as storage service is not initialized", this.keyspace.getName());
         } else {
            var4 = this.allViews().iterator();

            while(var4.hasNext()) {
               View view = (View)var4.next();
               view.build();
               view.updateDefinition((ViewMetadata)newViewsByName.get(view.name));
            }

         }
      }
   }

   public void addView(ViewMetadata definition) {
      if(!this.keyspace.hasColumnFamilyStore(definition.baseTableId)) {
         logger.warn("Not adding view {} because the base table {} is unknown", definition.name, definition.baseTableId);
      } else {
         View view = new View(definition, this.keyspace.getColumnFamilyStore(definition.baseTableId));
         this.forTable(view.getDefinition().baseTableId).add(view);
         this.viewsByName.put(definition.name, view);
      }
   }

   public void dropView(String name) {
      View view = (View)this.viewsByName.remove(name);
      if(view != null) {
         view.stopBuild();
         this.forTable(view.getDefinition().baseTableId).removeByName(name);
         TPCUtils.blockingAwait(SystemKeyspace.setViewRemoved(this.keyspace.getName(), view.name));
         TPCUtils.blockingAwait(SystemDistributedKeyspace.setViewRemoved(this.keyspace.getName(), view.name));
      }
   }

   public View getByName(String name) {
      return (View)this.viewsByName.get(name);
   }

   public void buildAllViews() {
      Iterator var1 = this.allViews().iterator();

      while(var1.hasNext()) {
         View view = (View)var1.next();
         view.build();
      }

   }

   public TableViews forTable(TableId id) {
      TableViews views = (TableViews)this.viewsByBaseTable.get(id);
      if(views == null) {
         views = new TableViews(id);
         TableViews previous = (TableViews)this.viewsByBaseTable.putIfAbsent(id, views);
         if(previous != null) {
            views = previous;
         }
      }

      return views;
   }

   public CompletableFuture<Void> updateWithLocks(Mutation mutation, Supplier<CompletableFuture<Void>> update, boolean isDroppable) {
      if(mutation.viewLockAcquireStart == 0L) {
         mutation.viewLockAcquireStart = System.currentTimeMillis();
      }

      SortedMap<Long, ExecutableLock> locks = this.getLocksFor(mutation);
      Supplier<CompletableFuture<Void>> onLocksAcquired = () -> {
         if(isDroppable) {
            long acquireTime = System.currentTimeMillis() - mutation.viewLockAcquireStart;
            Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());
            Iterator var6 = mutation.getTableIds().iterator();

            while(var6.hasNext()) {
               TableId tableId = (TableId)var6.next();
               keyspace.getColumnFamilyStore(tableId).metric.viewLockAcquireTime.update(acquireTime, TimeUnit.MILLISECONDS);
            }
         }

         return (CompletableFuture)update.get();
      };
      Function<TimeoutException, RuntimeException> onTimeout = (err) -> {
         List<String> tables = new ArrayList(mutation.getTableIds().size());
         Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());
         Iterator var4 = mutation.getTableIds().iterator();

         while(var4.hasNext()) {
            TableId tableId = (TableId)var4.next();
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(tableId);
            cfs.metric.viewLockAcquisitionTimeouts.inc();
            tables.add(cfs.name);
         }

         String message = String.format("Could not acquire view lock in %d milliseconds for table%s %s of keyspace %s.", new Object[]{Long.valueOf(System.currentTimeMillis() - mutation.createdAt), tables.size() > 1?"s":"", Joiner.on(",").join(tables), keyspace.getName()});
         return new DroppingResponseException(message);
      };
      long timeoutMillis = isDroppable?DatabaseDescriptor.getWriteRpcTimeout():9223372036854775807L;
      return TPCUtils.withLocks(locks, mutation.createdAt, timeoutMillis, onLocksAcquired, onTimeout);
   }

   private SortedMap<Long, ExecutableLock> getLocksFor(Mutation mutation) {
      final SortedMap<Long, ExecutableLock> locks = mutation.getTableIds().stream().
              map(t -> Objects.hash(mutation.key().getKey(), t)).
              map(k -> this.getLockFor(k)).collect(
                      Collectors.toMap(p -> (Long)p.left,
                              p -> (ExecutableLock)p.right,
                              (v1, v2) -> v1,
                              () -> new TreeMap(
                                      new Comparator<Long>(){
                                         @Override
                                         public int compare(Long o1, Long o2) {
                                            return o1.compareTo(o2);
                                         }
                                      }//(k1, k2) -> k1.compareTo(k2))
                      )));
      return locks;
   }

   private Pair<Long, ExecutableLock> getLockFor(int keyAndCfidHash) {
      Semaphore semaphore = (Semaphore)SEMAPHORES.get(Integer.valueOf(keyAndCfidHash));
      return (Pair)LOCKS.computeIfAbsent(semaphore, (s) -> {
         return Pair.create(Long.valueOf(LOCK_ID_GEN.incrementAndGet()), new ExecutableLock(s));
      });
   }

   static {
      SEMAPHORES = Striped.lazyWeakSemaphore(LOCK_STRIPES, 1);
      LOCKS = new ConcurrentHashMap();
      LOCK_ID_GEN = new AtomicLong();
      enableCoordinatorBatchlog = Boolean.getBoolean("cassandra.mv_enable_coordinator_batchlog");
   }
}
