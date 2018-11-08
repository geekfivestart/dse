package org.apache.cassandra.db;

import java.net.InetAddress;
import org.apache.cassandra.utils.versioning.Version;

public enum DigestVersion implements Version<DigestVersion> {
   OSS_30;

   private DigestVersion() {
   }

   public static DigestVersion forReplicas(Iterable<InetAddress> replicas) {
      return OSS_30;
   }
}
