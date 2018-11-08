package org.apache.cassandra.db;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.util.concurrent.Striped;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.functions.Predicate;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.cassandra.concurrent.SchedulableMessage;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.concurrent.TracingAwareExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CounterId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.concurrent.ExecutableLock;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.RxThreads;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public class CounterMutation implements IMutation, SchedulableMessage {
   public static final Versioned<WriteVerbs.WriteVersion, Serializer<CounterMutation>> serializers = WriteVerbs.WriteVersion.versioned((x$0) -> {
      return new CounterMutation.CounterMutationSerializer(x$0);
   });
   private static final Striped<Semaphore> SEMAPHORES_STRIPED = Striped.semaphore(TPC.getNumCores() * 1024, 1);
   private static final ConcurrentMap<Semaphore, Pair<Long, ExecutableLock>> LOCKS = new ConcurrentHashMap();
   private static final AtomicLong LOCK_ID_GEN = new AtomicLong();
   private final Mutation mutation;
   private final ConsistencyLevel consistency;

   public CounterMutation(Mutation mutation, ConsistencyLevel consistency) {
      this.mutation = mutation;
      this.consistency = consistency;
   }

   public String getKeyspaceName() {
      return this.mutation.getKeyspaceName();
   }

   public Collection<TableId> getTableIds() {
      return this.mutation.getTableIds();
   }

   public Collection<PartitionUpdate> getPartitionUpdates() {
      return this.mutation.getPartitionUpdates();
   }

   public Mutation getMutation() {
      return this.mutation;
   }

   public DecoratedKey key() {
      return this.mutation.key();
   }

   public ConsistencyLevel consistency() {
      return this.consistency;
   }

   public CompletableFuture<Mutation> applyCounterMutation() {
      return this.applyCounterMutation(System.currentTimeMillis());
   }

   public StagedScheduler getScheduler() {
      return this.mutation.getScheduler();
   }

   public TracingAwareExecutor getRequestExecutor() {
      return this.mutation.getRequestExecutor();
   }

   public TracingAwareExecutor getResponseExecutor() {
      return this.mutation.getResponseExecutor();
   }

   private Single<Mutation> applyCounterMutationInternal() {
      Mutation result = new Mutation(this.getKeyspaceName(), this.key());
      Completable ret = Completable.concat((Iterable)this.getPartitionUpdates().stream().map(this::processModifications).map((single) -> {
         return single.flatMapCompletable((upd) -> {
            return Completable.fromRunnable(() -> {
               result.add(upd);
            });
         });
      }).collect(Collectors.toList()));
      return RxThreads.subscribeOn(ret.andThen(result.applyAsync()).toSingleDefault(result), this.getScheduler(), TPCTaskType.COUNTER_ACQUIRE_LOCK);
   }

   public void apply() {
      TPCUtils.blockingGet(this.applyCounterMutation());
   }

   public Completable applyAsync() {
      return TPCUtils.toCompletable(this.applyCounterMutation().thenAccept((mutation) -> {
      }));
   }

   private CompletableFuture<Mutation> applyCounterMutation(long startTime) {
      Tracing.trace("Acquiring counter locks");
      SortedMap<Long, ExecutableLock> locks = this.getLocks();
      return locks.isEmpty()?TPCUtils.toFuture(this.applyCounterMutationInternal()):TPCUtils.withLocks(locks, startTime, DatabaseDescriptor.getCounterWriteRpcTimeout(), () -> {
         return TPCUtils.toFuture(this.applyCounterMutationInternal());
      }, (err) -> {
         Tracing.trace("Failed to acquire locks for counter mutation for longer than {} millis, giving up", (Object)Long.valueOf(DatabaseDescriptor.getCounterWriteRpcTimeout()));
         Keyspace keyspace = Keyspace.open(this.getKeyspaceName());
         return new WriteTimeoutException(WriteType.COUNTER, this.consistency(), 0, this.consistency().blockFor(keyspace));
      });
   }

   private SortedMap<Long, ExecutableLock> getLocks() {
      SortedMap<Long, ExecutableLock> ret = new TreeMap(Comparator.naturalOrder());
      Iterator var2 = this.getPartitionUpdates().iterator();

      while(var2.hasNext()) {
         PartitionUpdate update = (PartitionUpdate)var2.next();
         Iterator var4 = update.iterator();

         while(var4.hasNext()) {
            Row row = (Row)var4.next();
            Iterator var6 = row.iterator();

            while(var6.hasNext()) {
               ColumnData data = (ColumnData)var6.next();
               int hash = Objects.hash(new Object[]{update.metadata().id, this.key(), row.clustering(), data.column()});
               Semaphore semaphore = (Semaphore)SEMAPHORES_STRIPED.get(Integer.valueOf(hash));
               Pair<Long, ExecutableLock> p = (Pair)LOCKS.computeIfAbsent(semaphore, (s) -> {
                  return Pair.create(Long.valueOf(LOCK_ID_GEN.incrementAndGet()), new ExecutableLock(s));
               });
               ret.put(p.left, p.right);
            }
         }
      }

      return ret;
   }

   private Single<PartitionUpdate> processModifications(PartitionUpdate changes) {
      ColumnFamilyStore cfs = Keyspace.open(this.getKeyspaceName()).getColumnFamilyStore(changes.metadata().id);
      List<PartitionUpdate.CounterMark> marks = changes.collectCounterMarks();
      if(CacheService.instance.counterCache.getCapacity() != 0L) {
         Tracing.trace("Fetching {} counter values from cache", (Object)Integer.valueOf(marks.size()));
         this.updateWithCurrentValuesFromCache(marks, cfs);
         if(marks.isEmpty()) {
            return Single.just(changes);
         }
      }

      Tracing.trace("Reading {} counter values from the CF", (Object)Integer.valueOf(marks.size()));
      return this.updateWithCurrentValuesFromCFS(marks, cfs).andThen(Single.fromCallable(() -> {
         Iterator var4 = marks.iterator();

         while(var4.hasNext()) {
            PartitionUpdate.CounterMark mark = (PartitionUpdate.CounterMark)var4.next();
            this.updateWithCurrentValue(mark, ClockAndCount.BLANK, cfs);
         }

         return changes;
      }));
   }

   private void updateWithCurrentValue(PartitionUpdate.CounterMark mark, ClockAndCount currentValue, ColumnFamilyStore cfs) {
      long clock = Math.max(FBUtilities.timestampMicros(), currentValue.clock + 1L);
      long count = currentValue.count + CounterContext.instance().total(mark.value());
      mark.setValue(CounterContext.instance().createGlobal(CounterId.getLocalId(), clock, count));
      cfs.putCachedCounter(this.key().getKey(), mark.clustering(), mark.column(), mark.path(), ClockAndCount.create(clock, count));
   }

   private void updateWithCurrentValuesFromCache(List<PartitionUpdate.CounterMark> marks, ColumnFamilyStore cfs) {
      Iterator iter = marks.iterator();

      while(iter.hasNext()) {
         PartitionUpdate.CounterMark mark = (PartitionUpdate.CounterMark)iter.next();
         ClockAndCount cached = cfs.getCachedCounter(this.key().getKey(), mark.clustering(), mark.column(), mark.path());
         if(cached != null) {
            this.updateWithCurrentValue(mark, cached, cfs);
            iter.remove();
         }
      }

   }

   private Completable updateWithCurrentValuesFromCFS(List<PartitionUpdate.CounterMark> marks, ColumnFamilyStore cfs) {
      ColumnFilter.Builder builder = ColumnFilter.selectionBuilder();
      BTreeSet.Builder<Clustering> names = BTreeSet.builder(cfs.metadata().comparator);
      Iterator var5 = marks.iterator();

      while(var5.hasNext()) {
         PartitionUpdate.CounterMark mark = (PartitionUpdate.CounterMark)var5.next();
         if(mark.clustering() != Clustering.STATIC_CLUSTERING) {
            names.add(mark.clustering());
         }

         if(mark.path() == null) {
            builder.add(mark.column());
         } else {
            builder.select(mark.column(), mark.path());
         }
      }

      int nowInSec = FBUtilities.nowInSeconds();
      ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(names.build(), false);
      SinglePartitionReadCommand cmd = SinglePartitionReadCommand.create(cfs.metadata(), nowInSec, this.key(), builder.build(), filter);
      PeekingIterator<PartitionUpdate.CounterMark> markIter = Iterators.peekingIterator(marks.iterator());
      return Completable.using(() -> {
         return cmd.executionController();
      }, (controller) -> {
         return cmd.deferredQuery(cfs, controller).flatMapCompletable((p) -> {
            FlowablePartition partition = FlowablePartitions.filter(p, nowInSec);
            this.updateForRow(markIter, partition.staticRow(), cfs);
            return partition.content().takeWhile((row) -> {
               return markIter.hasNext();
            }).processToRxCompletable((row) -> {
               this.updateForRow(markIter, row, cfs);
            });
         });
      }, (controller) -> {
         controller.close();
      });
   }

   private int compare(Clustering c1, Clustering c2, ColumnFamilyStore cfs) {
      return c1 == Clustering.STATIC_CLUSTERING?(c2 == Clustering.STATIC_CLUSTERING?0:-1):(c2 == Clustering.STATIC_CLUSTERING?1:cfs.getComparator().compare(c1, c2));
   }

   private void updateForRow(PeekingIterator<PartitionUpdate.CounterMark> markIter, Row row, ColumnFamilyStore cfs) {
      int cmp = 0;

      while(markIter.hasNext() && (cmp = this.compare(((PartitionUpdate.CounterMark)markIter.peek()).clustering(), row.clustering(), cfs)) < 0) {
         markIter.next();
      }

      if(markIter.hasNext()) {
         while(cmp == 0) {
            PartitionUpdate.CounterMark mark = (PartitionUpdate.CounterMark)markIter.next();
            Cell cell = mark.path() == null?row.getCell(mark.column()):row.getCell(mark.column(), mark.path());
            if(cell != null) {
               this.updateWithCurrentValue(mark, CounterContext.instance().getLocalClockAndCount(cell.value()), cfs);
               markIter.remove();
            }

            if(!markIter.hasNext()) {
               return;
            }

            cmp = this.compare(((PartitionUpdate.CounterMark)markIter.peek()).clustering(), row.clustering(), cfs);
         }

      }
   }

   public long getTimeout() {
      return DatabaseDescriptor.getCounterWriteRpcTimeout();
   }

   public String toString() {
      return this.toString(false);
   }

   public String toString(boolean shallow) {
      return String.format("CounterMutation(%s, %s)", new Object[]{this.mutation.toString(shallow), this.consistency});
   }

   private static class CounterMutationSerializer extends VersionDependent<WriteVerbs.WriteVersion> implements Serializer<CounterMutation> {
      private CounterMutationSerializer(WriteVerbs.WriteVersion version) {
         super(version);
      }

      public void serialize(CounterMutation cm, DataOutputPlus out) throws IOException {
         ((Serializer)Mutation.serializers.get(this.version)).serialize(cm.mutation, out);
         out.writeUTF(cm.consistency.name());
      }

      public CounterMutation deserialize(DataInputPlus in) throws IOException {
         Mutation m = (Mutation)((Serializer)Mutation.serializers.get(this.version)).deserialize(in);
         ConsistencyLevel consistency = (ConsistencyLevel)Enum.valueOf(ConsistencyLevel.class, in.readUTF());
         return new CounterMutation(m, consistency);
      }

      public long serializedSize(CounterMutation cm) {
         return ((Serializer)Mutation.serializers.get(this.version)).serializedSize(cm.mutation) + (long)TypeSizes.sizeof(cm.consistency.name());
      }
   }
}
