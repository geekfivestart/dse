package org.apache.cassandra.dht;

import org.apache.cassandra.utils.versioning.Version;

public enum BoundsVersion implements Version<BoundsVersion> {
   LEGACY,
   OSS_30;

   private BoundsVersion() {
   }
}
