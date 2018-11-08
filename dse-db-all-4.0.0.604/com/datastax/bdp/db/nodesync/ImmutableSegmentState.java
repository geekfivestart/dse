package com.datastax.bdp.db.nodesync;

class ImmutableSegmentState extends SegmentState {
   private final Segment segment;
   private final long lastValidationTimeMs;
   private final long lastSuccessfulValidationTimeMs;
   private final long deadlineTargetMs;
   private final boolean isLocallyLocked;
   private final boolean isRemotelyLocked;

   ImmutableSegmentState(Segment segment, long lastValidationTimeMs, long lastSuccessfulValidationTimeMs, long deadlineTargetMs, boolean isLocallyLocked, boolean isRemotelyLocked) {
      this.segment = segment;
      this.lastValidationTimeMs = lastValidationTimeMs;
      this.lastSuccessfulValidationTimeMs = lastSuccessfulValidationTimeMs;
      this.deadlineTargetMs = deadlineTargetMs;
      this.isLocallyLocked = isLocallyLocked;
      this.isRemotelyLocked = isRemotelyLocked;
   }

   Segment segment() {
      return this.segment;
   }

   long lastValidationTimeMs() {
      return this.lastValidationTimeMs;
   }

   long lastSuccessfulValidationTimeMs() {
      return this.lastSuccessfulValidationTimeMs;
   }

   long deadlineTargetMs() {
      return this.deadlineTargetMs;
   }

   boolean isLocallyLocked() {
      return this.isLocallyLocked;
   }

   boolean isRemotelyLocked() {
      return this.isRemotelyLocked;
   }
}
