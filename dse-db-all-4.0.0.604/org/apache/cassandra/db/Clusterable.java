package org.apache.cassandra.db;

public interface Clusterable {
   ClusteringPrefix clustering();
}
