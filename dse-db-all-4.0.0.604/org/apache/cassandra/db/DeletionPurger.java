package org.apache.cassandra.db;

public interface DeletionPurger {
   DeletionPurger PURGE_ALL = (ts, ldt) -> {
      return true;
   };

   boolean shouldPurge(long var1, int var3);

   default boolean shouldPurge(DeletionTime dt) {
      return !dt.isLive() && this.shouldPurge(dt.markedForDeleteAt(), dt.localDeletionTime());
   }

   default boolean shouldPurge(LivenessInfo liveness, int nowInSec) {
      return !liveness.isLive(nowInSec) && this.shouldPurge(liveness.timestamp(), liveness.localExpirationTime());
   }
}
