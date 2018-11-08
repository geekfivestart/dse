package org.apache.cassandra.repair;

import java.util.Collection;
import java.util.UUID;

public class RepairSessionResult {
   public final UUID sessionId;
   public final String keyspace;
   public final Collection<RepairResult> repairJobResults;

   public RepairSessionResult(UUID sessionId, String keyspace, Collection<RepairResult> repairJobResults) {
      this.sessionId = sessionId;
      this.keyspace = keyspace;
      this.repairJobResults = repairJobResults;
   }

   public String toString() {
      return "RepairSessionResult{sessionId=" + this.sessionId + ", keyspace='" + this.keyspace + '\'' + ", repairJobResults=" + this.repairJobResults + '}';
   }
}
