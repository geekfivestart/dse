package com.datastax.bdp.reporting.snapshots.histograms;

import org.apache.cassandra.db.ColumnFamilyStore;

public interface CfsHistogramDataProvider {
   long[] getHistogramData(ColumnFamilyStore var1);
}
