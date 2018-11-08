package org.apache.cassandra.db.lifecycle;

import org.apache.cassandra.db.ColumnFamilyStore;

public class IntervalTreeUtil {
   public IntervalTreeUtil() {
   }

   public static SSTableIntervalTree getIntervalTree(ColumnFamilyStore cfs) {
      return cfs.getTracker().getView().intervalTree;
   }
}
