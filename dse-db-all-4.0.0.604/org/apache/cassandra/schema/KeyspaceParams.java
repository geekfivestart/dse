package org.apache.cassandra.schema;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import java.util.Map;
import java.util.Objects;

public final class KeyspaceParams {
   public static final boolean DEFAULT_DURABLE_WRITES = true;
   @VisibleForTesting
   public static boolean DEFAULT_LOCAL_DURABLE_WRITES = true;
   public final boolean durableWrites;
   public final ReplicationParams replication;

   public KeyspaceParams(boolean durableWrites, ReplicationParams replication) {
      this.durableWrites = durableWrites;
      this.replication = replication;
   }

   public static KeyspaceParams create(boolean durableWrites, Map<String, String> replication) {
      return new KeyspaceParams(durableWrites, ReplicationParams.fromMap(replication));
   }

   public static KeyspaceParams local() {
      return new KeyspaceParams(DEFAULT_LOCAL_DURABLE_WRITES, ReplicationParams.local());
   }

   public static KeyspaceParams simple(int replicationFactor) {
      return new KeyspaceParams(true, ReplicationParams.simple(replicationFactor));
   }

   public static KeyspaceParams simpleTransient(int replicationFactor) {
      return new KeyspaceParams(false, ReplicationParams.simple(replicationFactor));
   }

   public static KeyspaceParams nts(Object... args) {
      return new KeyspaceParams(true, ReplicationParams.nts(args));
   }

   public void validate(String name) {
      this.replication.validate(name);
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof KeyspaceParams)) {
         return false;
      } else {
         KeyspaceParams p = (KeyspaceParams)o;
         return this.durableWrites == p.durableWrites && this.replication.equals(p.replication);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{Boolean.valueOf(this.durableWrites), this.replication});
   }

   public String toString() {
      return MoreObjects.toStringHelper(this).add(KeyspaceParams.Option.DURABLE_WRITES.toString(), this.durableWrites).add(KeyspaceParams.Option.REPLICATION.toString(), this.replication).toString();
   }

   public static enum Option {
      DURABLE_WRITES,
      REPLICATION;

      private Option() {
      }

      public String toString() {
         return this.name().toLowerCase();
      }
   }
}
