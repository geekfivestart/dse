package org.apache.cassandra.db.lifecycle;

import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class SSTableIntervalTree extends IntervalTree<PartitionPosition, SSTableReader, Interval<PartitionPosition, SSTableReader>> {
   private static final SSTableIntervalTree EMPTY = new SSTableIntervalTree((Collection)null);

   SSTableIntervalTree(Collection<Interval<PartitionPosition, SSTableReader>> intervals) {
      super(intervals);
   }

   public static SSTableIntervalTree empty() {
      return EMPTY;
   }

   public static SSTableIntervalTree build(Iterable<SSTableReader> sstables) {
      return new SSTableIntervalTree(buildIntervals(sstables));
   }

   public static List<Interval<PartitionPosition, SSTableReader>> buildIntervals(Iterable<SSTableReader> sstables) {
      List<Interval<PartitionPosition, SSTableReader>> intervals = new ArrayList(Iterables.size(sstables));
      Iterator var2 = sstables.iterator();

      while(var2.hasNext()) {
         SSTableReader sstable = (SSTableReader)var2.next();
         intervals.add(Interval.create(sstable.first, sstable.last, sstable));
      }

      return intervals;
   }
}
