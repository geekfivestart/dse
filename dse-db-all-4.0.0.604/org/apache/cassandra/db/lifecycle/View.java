package org.apache.cassandra.db.lifecycle;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Interval;

public class View {
   public final List<Memtable> liveMemtables;
   public final List<Memtable> flushingMemtables;
   final Set<SSTableReader> compacting;
   final Set<SSTableReader> sstables;
   final Map<SSTableReader, SSTableReader> sstablesMap;
   final Map<SSTableReader, SSTableReader> compactingMap;
   final SSTableIntervalTree intervalTree;

   View(List<Memtable> liveMemtables, List<Memtable> flushingMemtables, Map<SSTableReader, SSTableReader> sstables, Map<SSTableReader, SSTableReader> compacting, SSTableIntervalTree intervalTree) {
      assert liveMemtables != null;

      assert flushingMemtables != null;

      assert sstables != null;

      assert compacting != null;

      assert intervalTree != null;

      this.liveMemtables = liveMemtables;
      this.flushingMemtables = flushingMemtables;
      this.sstablesMap = sstables;
      this.sstables = this.sstablesMap.keySet();
      this.compactingMap = compacting;
      this.compacting = this.compactingMap.keySet();
      this.intervalTree = intervalTree;
   }

   public Memtable getCurrentMemtable() {
      return (Memtable)this.liveMemtables.get(this.liveMemtables.size() - 1);
   }

   public Iterable<Memtable> getAllMemtables() {
      return Iterables.concat(this.flushingMemtables, this.liveMemtables);
   }

   public Set<SSTableReader> liveSSTables() {
      return this.sstables;
   }

   public Iterable<SSTableReader> sstables(SSTableSet sstableSet, Predicate<SSTableReader> filter) {
      return Iterables.filter(this.select(sstableSet), filter);
   }

   @VisibleForTesting
   public Iterable<SSTableReader> allKnownSSTables() {
      return Iterables.concat(this.sstables, Helpers.filterOut(this.compacting, new Set[]{this.sstables}));
   }

   public Iterable<SSTableReader> select(SSTableSet sstableSet) {
      switch(null.$SwitchMap$org$apache$cassandra$db$lifecycle$SSTableSet[sstableSet.ordinal()]) {
      case 1:
         return this.sstables;
      case 2:
         return Iterables.filter(this.sstables, (s) -> {
            return !this.compacting.contains(s);
         });
      case 3:
         Set<SSTableReader> canonicalSSTables = new HashSet();
         Iterator var3 = this.compacting.iterator();

         SSTableReader sstable;
         while(var3.hasNext()) {
            sstable = (SSTableReader)var3.next();
            if(sstable.openReason != SSTableReader.OpenReason.EARLY) {
               canonicalSSTables.add(sstable);
            }
         }

         var3 = this.sstables.iterator();

         while(var3.hasNext()) {
            sstable = (SSTableReader)var3.next();
            if(!this.compacting.contains(sstable) && sstable.openReason != SSTableReader.OpenReason.EARLY) {
               canonicalSSTables.add(sstable);
            }
         }

         return canonicalSSTables;
      default:
         throw new IllegalStateException();
      }
   }

   public Iterable<SSTableReader> getUncompacting(Iterable<SSTableReader> candidates) {
      return Iterables.filter(candidates, new Predicate<SSTableReader>() {
         public boolean apply(SSTableReader sstable) {
            return !View.this.compacting.contains(sstable);
         }
      });
   }

   public boolean isEmpty() {
      return this.sstables.isEmpty() && this.liveMemtables.size() <= 1 && this.flushingMemtables.size() == 0 && (this.liveMemtables.size() == 0 || ((Memtable)this.liveMemtables.get(0)).getOperations() == 0L);
   }

   public String toString() {
      return String.format("View(pending_count=%d, sstables=%s, compacting=%s)", new Object[]{Integer.valueOf(this.liveMemtables.size() + this.flushingMemtables.size() - 1), this.sstables, this.compacting});
   }

   public Iterable<SSTableReader> liveSSTablesInBounds(PartitionPosition left, PartitionPosition right) {
      assert !AbstractBounds.strictlyWrapsAround(left, right);

      if(this.intervalTree.isEmpty()) {
         return Collections.emptyList();
      } else {
         PartitionPosition stopInTree = right.isMinimum()?(PartitionPosition)this.intervalTree.max():right;
         return this.intervalTree.search(Interval.create(left, stopInTree));
      }
   }

   public static List<SSTableReader> sstablesInBounds(PartitionPosition left, PartitionPosition right, SSTableIntervalTree intervalTree) {
      assert !AbstractBounds.strictlyWrapsAround(left, right);

      if(intervalTree.isEmpty()) {
         return Collections.emptyList();
      } else {
         PartitionPosition stopInTree = right.isMinimum()?(PartitionPosition)intervalTree.max():right;
         return intervalTree.search(Interval.create(left, stopInTree));
      }
   }

   public static Function<View, Iterable<SSTableReader>> selectFunction(SSTableSet sstableSet) {
      return (view) -> {
         return view.select(sstableSet);
      };
   }

   public static Function<View, Iterable<SSTableReader>> select(SSTableSet sstableSet, Predicate<SSTableReader> filter) {
      return (view) -> {
         return view.sstables(sstableSet, filter);
      };
   }

   public static Function<View, Iterable<SSTableReader>> select(SSTableSet sstableSet, DecoratedKey key) {
      assert sstableSet == SSTableSet.LIVE;

      return (view) -> {
         return view.intervalTree.search(key);
      };
   }

   public static Function<View, Iterable<SSTableReader>> selectLive(AbstractBounds<PartitionPosition> rowBounds) {
      return (view) -> {
         return view.liveSSTablesInBounds((PartitionPosition)rowBounds.left, (PartitionPosition)rowBounds.right);
      };
   }

   static Function<View, View> updateCompacting(final Set<SSTableReader> unmark, final Iterable<SSTableReader> mark) {
      return unmark.isEmpty() && Iterables.isEmpty(mark)?Functions.identity():new Function<View, View>() {
         public View apply(View view) {
            assert Iterables.all(mark, Helpers.idIn(view.sstablesMap));

            return new View(view.liveMemtables, view.flushingMemtables, view.sstablesMap, Helpers.replace(view.compactingMap, unmark, mark), view.intervalTree);
         }
      };
   }

   static Predicate<View> permitCompacting(final Iterable<SSTableReader> readers) {
      return new Predicate<View>() {
         public boolean apply(View view) {
            Iterator var2 = readers.iterator();

            SSTableReader reader;
            do {
               if(!var2.hasNext()) {
                  return true;
               }

               reader = (SSTableReader)var2.next();
            } while(!view.compacting.contains(reader) && view.sstablesMap.get(reader) == reader && !reader.isMarkedCompacted());

            return false;
         }
      };
   }

   static Function<View, View> updateLiveSet(final Set<SSTableReader> remove, final Iterable<SSTableReader> add) {
      return remove.isEmpty() && Iterables.isEmpty(add)?Functions.identity():new Function<View, View>() {
         public View apply(View view) {
            Map<SSTableReader, SSTableReader> sstableMap = Helpers.replace(view.sstablesMap, remove, add);
            return new View(view.liveMemtables, view.flushingMemtables, sstableMap, view.compactingMap, SSTableIntervalTree.build(sstableMap.keySet()));
         }
      };
   }

   static Function<View, View> switchMemtable(final Memtable newMemtable) {
      return new Function<View, View>() {
         public View apply(View view) {
            List<Memtable> newLive = ImmutableList.builder().addAll(view.liveMemtables).add(newMemtable).build();

            assert newLive.size() == view.liveMemtables.size() + 1;

            return new View(newLive, view.flushingMemtables, view.sstablesMap, view.compactingMap, view.intervalTree);
         }
      };
   }

   static Function<View, View> markFlushing(final Memtable toFlush) {
      return new Function<View, View>() {
         public View apply(View view) {
            List<Memtable> live = view.liveMemtables;
            List<Memtable> flushing = view.flushingMemtables;
            List<Memtable> newLive = ImmutableList.copyOf(Iterables.filter(live, Predicates.not(Predicates.equalTo(toFlush))));
            List<Memtable> newFlushing = ImmutableList.copyOf(Iterables.concat(Iterables.filter(flushing, View.lessThan(toFlush)), ImmutableList.of(toFlush), Iterables.filter(flushing, Predicates.not(View.lessThan(toFlush)))));

            assert newLive.size() == live.size() - 1;

            assert newFlushing.size() == flushing.size() + 1;

            return new View(newLive, newFlushing, view.sstablesMap, view.compactingMap, view.intervalTree);
         }
      };
   }

   static Function<View, View> replaceFlushed(final Memtable memtable, final Iterable<SSTableReader> flushed) {
      return new Function<View, View>() {
         public View apply(View view) {
            List<Memtable> flushingMemtables = ImmutableList.copyOf(Iterables.filter(view.flushingMemtables, Predicates.not(Predicates.equalTo(memtable))));

            assert flushingMemtables.size() == view.flushingMemtables.size() - 1;

            if(flushed != null && !Iterables.isEmpty(flushed)) {
               Map<SSTableReader, SSTableReader> sstableMap = Helpers.replace(view.sstablesMap, Helpers.emptySet(), flushed);
               return new View(view.liveMemtables, flushingMemtables, sstableMap, view.compactingMap, SSTableIntervalTree.build(sstableMap.keySet()));
            } else {
               return new View(view.liveMemtables, flushingMemtables, view.sstablesMap, view.compactingMap, view.intervalTree);
            }
         }
      };
   }

   private static <T extends Comparable<T>> Predicate<T> lessThan(final T lessThan) {
      return new Predicate<T>() {
         public boolean apply(T t) {
            return t.compareTo(lessThan) < 0;
         }
      };
   }
}
