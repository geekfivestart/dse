package com.datastax.bdp.reporting.snapshots.histograms;

import org.apache.cassandra.db.Keyspace;

public interface KsHistogramDataProvider {
   long[] getHistogramData(Keyspace var1);
}
