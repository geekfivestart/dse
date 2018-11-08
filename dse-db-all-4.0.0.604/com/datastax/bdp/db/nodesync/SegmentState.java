package com.datastax.bdp.db.nodesync;

import java.util.concurrent.TimeUnit;
import org.apache.cassandra.utils.units.Units;

abstract class SegmentState {
   private static final long REMOTE_LOCK_PRIORITY_PENALTY_MS;
   private static final long LOCAL_LOCK_PRIORITY_PENALTY_MS;

   SegmentState() {
   }

   abstract Segment segment();

   abstract long lastValidationTimeMs();

   abstract long lastSuccessfulValidationTimeMs();

   abstract long deadlineTargetMs();

   abstract boolean isLocallyLocked();

   abstract boolean isRemotelyLocked();

   boolean lastValidationWasSuccessful() {
      return this.lastValidationTimeMs() == this.lastSuccessfulValidationTimeMs();
   }

   long priority() {
      return priority(this.lastValidationTimeMs(), this.lastSuccessfulValidationTimeMs(), this.deadlineTargetMs(), this.isLocallyLocked(), this.isRemotelyLocked());
   }

   static long priority(long lastValidationTimeMs, long lastSuccessfulValidationTimeMs, long deadlineTargetMs, boolean locallyLocked, boolean remotelyLocked) {
      long priority = rawPriority(lastValidationTimeMs, lastSuccessfulValidationTimeMs, deadlineTargetMs);
      if(locallyLocked) {
         priority += LOCAL_LOCK_PRIORITY_PENALTY_MS;
      }

      if(remotelyLocked) {
         priority += REMOTE_LOCK_PRIORITY_PENALTY_MS;
      }

      return priority;
   }

   private static long rawPriority(long lastValidationTimeMs, long lastSuccessfulValidationTimeMs, long deadlineTargetMs) {
      long priority = lastValidationTimeMs + deadlineTargetMs;
      if(lastValidationTimeMs == lastSuccessfulValidationTimeMs) {
         return priority;
      } else {
         long diff = (lastValidationTimeMs - lastSuccessfulValidationTimeMs) / 2L;
         return priority - Math.max(5000L, Math.min(9L * deadlineTargetMs / 10L, diff));
      }
   }

   String toTraceString() {
      long last = this.lastValidationTimeMs();
      long lastSucc = this.lastSuccessfulValidationTimeMs();
      return last < 0L && lastSucc < 0L?"never validated":(last == lastSucc?String.format("validated %s", new Object[]{NodeSyncHelpers.sinceStr(last)}):(lastSucc < 0L?String.format("validated unsuccessfully %s (no known success)", new Object[]{NodeSyncHelpers.sinceStr(last)}):String.format("validated unsuccessfully %s (last success: %s)", new Object[]{NodeSyncHelpers.sinceStr(last), NodeSyncHelpers.sinceStr(lastSucc)})));
   }

   public String toString() {
      String deadlineStr = Units.toString(this.deadlineTargetMs(), TimeUnit.MILLISECONDS);
      long last = this.lastValidationTimeMs();
      long lastSucc = this.lastSuccessfulValidationTimeMs();
      String lockString = this.isLocallyLocked()?"[L. locked]":(this.isRemotelyLocked()?"[R. locked]":"");
      return last < 0L && lastSucc < 0L?String.format("%s(<never validated>)%s", new Object[]{this.segment(), lockString}):(this.lastValidationWasSuccessful()?String.format("%s(last validation=%s (successful), deadline=%s)%s", new Object[]{this.segment(), NodeSyncHelpers.sinceStr(last), deadlineStr, lockString}):String.format("%s(last validation=%s, last successful one=%s, deadline=%s)%s", new Object[]{this.segment(), NodeSyncHelpers.sinceStr(last), NodeSyncHelpers.sinceStr(lastSucc), deadlineStr, lockString}));
   }

   static {
      REMOTE_LOCK_PRIORITY_PENALTY_MS = Long.getLong("dse.nodesync.remote_lock_penalty_ms", TimeUnit.SECONDS.toMillis((long)ValidationLifecycle.LOCK_TIMEOUT_SEC)).longValue();
      LOCAL_LOCK_PRIORITY_PENALTY_MS = Long.getLong("dse.nodesync.local_lock_penalty_ms", 100L * REMOTE_LOCK_PRIORITY_PENALTY_MS).longValue();
   }
}
