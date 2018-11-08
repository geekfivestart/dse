package org.apache.cassandra.db.view;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import io.reactivex.Completable;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.ThrottledUnfilteredIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;

public class TableViews extends AbstractCollection<View> {
   private final TableMetadataRef baseTableMetadata;
   private final List<View> views = new CopyOnWriteArrayList();
   private static final int VIEW_REBUILD_BATCH_SIZE = Integer.getInteger("cassandra.view.rebuild.batch", 100).intValue();

   public TableViews(TableId id) {
      this.baseTableMetadata = Schema.instance.getTableMetadataRef(id);
   }

   public int size() {
      return this.views.size();
   }

   public Iterator<View> iterator() {
      return this.views.iterator();
   }

   public boolean contains(String viewName) {
      return Iterables.any(this.views, (view) -> {
         return view.name.equals(viewName);
      });
   }

   public boolean add(View view) {
      assert !this.contains(view.name);

      return this.views.add(view);
   }

   public Iterable<ColumnFamilyStore> allViewsCfs() {
      Keyspace keyspace = Keyspace.open(this.baseTableMetadata.keyspace);
      return Iterables.transform(this.views, (view) -> {
         return keyspace.getColumnFamilyStore(view.getDefinition().name);
      });
   }

   public void forceBlockingFlush() {
      Iterator var1 = this.allViewsCfs().iterator();

      while(var1.hasNext()) {
         ColumnFamilyStore viewCfs = (ColumnFamilyStore)var1.next();
         viewCfs.forceBlockingFlush();
      }

   }

   public void dumpMemtables() {
      Iterator var1 = this.allViewsCfs().iterator();

      while(var1.hasNext()) {
         ColumnFamilyStore viewCfs = (ColumnFamilyStore)var1.next();
         viewCfs.dumpMemtable();
      }

   }

   public void truncateBlocking(CommitLogPosition replayAfter, long truncatedAt) {
      Iterator var4 = this.allViewsCfs().iterator();

      while(var4.hasNext()) {
         ColumnFamilyStore viewCfs = (ColumnFamilyStore)var4.next();
         viewCfs.discardSSTables(truncatedAt);
         TPCUtils.blockingAwait(SystemKeyspace.saveTruncationRecord(viewCfs, truncatedAt, replayAfter));
      }

   }

   public void removeByName(String viewName) {
      this.views.removeIf((v) -> {
         return v.name.equals(viewName);
      });
   }

   public Completable pushViewReplicaUpdates(PartitionUpdate update, boolean writeCommitLog, AtomicLong baseComplete) {
      assert update.metadata().id.equals(this.baseTableMetadata.id);

      Collection<View> views = this.updatedViews(update);
      if(views.isEmpty()) {
         return Completable.complete();
      } else {
         int nowInSec = FBUtilities.nowInSeconds();
         long queryStartNanoTime = System.nanoTime();
         return ViewUpdateFlow.forUpdate(update, this.baseTableMetadata, views, nowInSec).flatMapCompletable((mutations) -> {
            return !mutations.isEmpty()?StorageProxy.mutateMV(update.partitionKey().getKey(), mutations, writeCommitLog, baseComplete, queryStartNanoTime):Completable.complete();
         });
      }
   }

   public Iterator<Collection<Mutation>> generateViewUpdates(final Collection<View> views, UnfilteredRowIterator updates, final int nowInSec) {
      assert updates.metadata().id.equals(this.baseTableMetadata.id);

      final CloseableIterator<UnfilteredRowIterator> updateIterators = ThrottledUnfilteredIterator.throttle(updates, VIEW_REBUILD_BATCH_SIZE);
      return new Iterator<Collection<Mutation>>() {
         public boolean hasNext() {
            return updateIterators.hasNext();
         }

         public Collection<Mutation> next() {
            return (Collection)ViewUpdateFlow.forRebuild((UnfilteredRowIterator)updateIterators.next(), TableViews.this.baseTableMetadata, views, nowInSec).blockingGet();
         }
      };
   }

   public Collection<View> updatedViews(PartitionUpdate updates) {
      List<View> matchingViews = new ArrayList(this.views.size());
      Iterator var3 = this.views.iterator();

      while(var3.hasNext()) {
         View view = (View)var3.next();
         ReadQuery selectQuery = view.getReadQuery();
         if(selectQuery.selectsKey(updates.partitionKey())) {
            matchingViews.add(view);
         }
      }

      return matchingViews;
   }
}
