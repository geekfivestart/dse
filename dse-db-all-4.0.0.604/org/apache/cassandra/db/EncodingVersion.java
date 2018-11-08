package org.apache.cassandra.db;

import java.util.function.Function;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;

public enum EncodingVersion implements Version<EncodingVersion> {
   OSS_30(ClusteringVersion.OSS_30);

   public final ClusteringVersion clusteringVersion;
   private static EncodingVersion last = null;

   private EncodingVersion(ClusteringVersion clusteringVersion) {
      this.clusteringVersion = clusteringVersion;
   }

   public static EncodingVersion last() {
      if(last == null) {
         last = values()[values().length - 1];
      }

      return last;
   }

   public static <T> Versioned<EncodingVersion, T> versioned(Function<EncodingVersion, ? extends T> function) {
      return new Versioned(EncodingVersion.class, function);
   }
}
