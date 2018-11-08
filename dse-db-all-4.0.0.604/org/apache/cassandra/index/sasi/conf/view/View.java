package org.apache.cassandra.index.sasi.conf.view;

import com.google.common.collect.Iterables;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sasi.SSTableIndex;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class View implements Iterable<SSTableIndex> {
   private final Map<Descriptor, SSTableIndex> view;
   private final TermTree termTree;
   private final AbstractType<?> keyValidator;
   private final IntervalTree<View.Key, SSTableIndex, Interval<View.Key, SSTableIndex>> keyIntervalTree;

   public View(ColumnIndex index, Set<SSTableIndex> indexes) {
      this(index, Collections.emptyList(), Collections.emptyList(), indexes);
   }

   public View(ColumnIndex index, Collection<SSTableIndex> currentView, Collection<SSTableReader> oldSSTables, Set<SSTableIndex> newIndexes) {
      Map<Descriptor, SSTableIndex> newView = new HashMap();
      AbstractType<?> validator = index.getValidator();
      TermTree.Builder termTreeBuilder = !(validator instanceof AsciiType) && !(validator instanceof UTF8Type)?new RangeTermTree.Builder(index.getMode().mode, validator):new PrefixTermTree.Builder(index.getMode().mode, validator);
      List<Interval<View.Key, SSTableIndex>> keyIntervals = new ArrayList();
      Iterator var9 = Iterables.concat(currentView, newIndexes).iterator();

      while(true) {
         while(var9.hasNext()) {
            SSTableIndex sstableIndex = (SSTableIndex)var9.next();
            SSTableReader sstable = sstableIndex.getSSTable();
            if(!oldSSTables.contains(sstable) && !sstable.isMarkedCompacted() && !newView.containsKey(sstable.descriptor)) {
               newView.put(sstable.descriptor, sstableIndex);
               ((TermTree.Builder)termTreeBuilder).add(sstableIndex);
               keyIntervals.add(Interval.create(new View.Key(sstableIndex.minKey(), index.keyValidator()), new View.Key(sstableIndex.maxKey(), index.keyValidator()), sstableIndex));
            } else {
               sstableIndex.release();
            }
         }

         this.view = newView;
         this.termTree = ((TermTree.Builder)termTreeBuilder).build();
         this.keyValidator = index.keyValidator();
         this.keyIntervalTree = IntervalTree.build(keyIntervals);
         if(this.keyIntervalTree.intervalCount() != this.termTree.intervalCount()) {
            throw new IllegalStateException(String.format("mismatched sizes for intervals tree for keys vs terms: %d != %d", new Object[]{Integer.valueOf(this.keyIntervalTree.intervalCount()), Integer.valueOf(this.termTree.intervalCount())}));
         }

         return;
      }
   }

   public Set<SSTableIndex> match(Expression expression) {
      return this.termTree.search(expression);
   }

   public List<SSTableIndex> match(ByteBuffer minKey, ByteBuffer maxKey) {
      return this.keyIntervalTree.search(Interval.create(new View.Key(minKey, this.keyValidator), new View.Key(maxKey, this.keyValidator), (SSTableIndex)null));
   }

   public Iterator<SSTableIndex> iterator() {
      return this.view.values().iterator();
   }

   public Collection<SSTableIndex> getIndexes() {
      return this.view.values();
   }

   private static class Key implements Comparable<View.Key> {
      private final ByteBuffer key;
      private final AbstractType<?> comparator;

      public Key(ByteBuffer key, AbstractType<?> comparator) {
         this.key = key;
         this.comparator = comparator;
      }

      public int compareTo(View.Key o) {
         return this.comparator.compare(this.key, o.key);
      }
   }
}
