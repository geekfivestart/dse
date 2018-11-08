package org.apache.cassandra.db;

import org.apache.cassandra.utils.versioning.Version;

public enum ClusteringVersion implements Version<ClusteringVersion> {
   OSS_30;

   private ClusteringVersion() {
   }
}
