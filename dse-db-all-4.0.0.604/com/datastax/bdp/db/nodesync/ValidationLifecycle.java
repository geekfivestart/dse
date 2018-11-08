package com.datastax.bdp.db.nodesync;

import java.util.concurrent.TimeUnit;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.Units;

class ValidationLifecycle {
   static final int LOCK_TIMEOUT_SEC;
   private final TableState.Ref segmentRef;
   private final NodeSyncTracing.SegmentTracing tracing;
   private final long startTime;
   private volatile int nextLockRefreshTimeSec;

   private ValidationLifecycle(TableState.Ref segmentRef, NodeSyncTracing.SegmentTracing tracing, long startTime) {
      this.segmentRef = segmentRef;
      this.tracing = tracing;
      this.startTime = startTime;
      this.nextLockRefreshTimeSec = computeNextLockRefresh((int)(startTime / 1000L));
   }

   static ValidationLifecycle createAndStart(TableState.Ref segmentRef, NodeSyncTracing.SegmentTracing tracing) {
      ValidationLifecycle lifecycle = new ValidationLifecycle(segmentRef, tracing, NodeSyncHelpers.time().currentTimeMillis());
      lifecycle.onStart();
      return lifecycle;
   }

   Segment segment() {
      return this.segmentRef.segment();
   }

   NodeSyncService service() {
      return this.segmentRef.service();
   }

   NodeSyncTracing.SegmentTracing tracing() {
      return this.tracing;
   }

   long startTime() {
      return this.startTime;
   }

   private NodeSyncStatusTableProxy statusTable() {
      return this.service().statusTableProxy;
   }

   private void onStart() {
      this.statusTable().lockNodeSyncSegment(this.segment(), (long)LOCK_TIMEOUT_SEC, TimeUnit.SECONDS);
      this.segmentRef.lock();
   }

   private void checkForInvalidation() {
      if(this.segmentRef.isInvalidated()) {
         throw new InvalidatedNodeSyncStateException();
      }
   }

   protected boolean isInvalid() {
      return this.segmentRef.isInvalidated();
   }

   void onNewPage(PageSize pageSize) {
      this.checkForInvalidation();
      this.tracing.trace("Querying new page (of max {})", (Object)pageSize);
      int nowInSec = NodeSyncHelpers.time().currentTimeSeconds();
      if(nowInSec > this.nextLockRefreshTimeSec) {
         this.tracing.trace("Refreshing lock on validation");
         this.statusTable().lockNodeSyncSegment(this.segment(), (long)LOCK_TIMEOUT_SEC, TimeUnit.SECONDS);
         this.segmentRef.refreshLock();
         this.nextLockRefreshTimeSec = computeNextLockRefresh(nowInSec);
      }

   }

   void onCompletedPage(ValidationOutcome outcome, ValidationMetrics pageMetrics) {
      if(this.tracing.isEnabled()) {
         this.tracing.trace("Page completed: outcome={}, validated={}, repaired={}", new Object[]{outcome, Units.toString(pageMetrics.dataValidated(), SizeUnit.BYTES), Units.toString(pageMetrics.dataRepaired(), SizeUnit.BYTES)});
      }

   }

   void onCompletion(ValidationInfo info, ValidationMetrics metrics) {
      this.tracing.onSegmentCompletion(info.outcome, metrics);
      this.statusTable().recordNodeSyncValidation(this.segment(), info, this.segmentRef.segmentStateAtCreation().lastValidationWasSuccessful());
      this.segmentRef.onCompletedValidation(info.startedAt, info.wasSuccessful());
   }

   void cancel(String reason) {
      this.tracing.trace("Cancelling validation: {}", (Object)reason);
      this.statusTable().forceReleaseNodeSyncSegmentLock(this.segment());
      this.segmentRef.forceUnlock();
   }

   private static int computeNextLockRefresh(int nowInSec) {
      return nowInSec + 3 * LOCK_TIMEOUT_SEC / 4;
   }

   static {
      LOCK_TIMEOUT_SEC = Integer.getInteger("dse.nodesync.segment_lock_timeout_sec", (int)TimeUnit.MINUTES.toSeconds(10L)).intValue();
   }
}
