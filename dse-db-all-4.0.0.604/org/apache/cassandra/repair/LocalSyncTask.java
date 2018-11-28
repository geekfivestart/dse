package org.apache.cassandra.repair;

import com.google.common.annotations.VisibleForTesting;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.RangeHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalSyncTask extends SyncTask implements StreamEventHandler {
   private final TraceState state;
   private static final Logger logger = LoggerFactory.getLogger(LocalSyncTask.class);
   private final UUID pendingRepair;
   private final boolean pullRepair;

   public LocalSyncTask(RepairJobDesc desc, TreeResponse r1, TreeResponse r2, UUID pendingRepair, boolean pullRepair, Executor taskExecutor, SyncTask next, Map<InetAddress, Set<RangeHash>> receivedRangeCache, PreviewKind previewKind) {
      super(desc, r1, r2, taskExecutor, next, receivedRangeCache, previewKind);
      this.state = Tracing.instance.get();
      this.pendingRepair = pendingRepair;
      this.pullRepair = pullRepair;
   }

   @VisibleForTesting
   StreamPlan createStreamPlan(InetAddress dst, InetAddress preferred, List<Range<Token>> toRequest, List<Range<Token>> toTransfer) {
      StreamPlan plan = (new StreamPlan(StreamOperation.REPAIR, 1, false, false, this.pendingRepair, this.previewKind)).listeners(this, new StreamEventHandler[0]).flushBeforeTransfer(this.pendingRepair == null).requestRanges(dst, preferred, this.desc.keyspace, toRequest, new String[]{this.desc.columnFamily});
      if(!this.pullRepair) {
         plan.transferRanges(dst, preferred, this.desc.keyspace, toTransfer, new String[]{this.desc.columnFamily});
      }

      return plan;
   }

   protected void startSync(List<Range<Token>> transferToLeft, List<Range<Token>> transferToRight) {
      InetAddress local = FBUtilities.getBroadcastAddress();
      InetAddress dst = this.endpoint2.equals(local)?this.endpoint1:this.endpoint2;
      List<Range<Token>> toRequest = this.endpoint2.equals(local)?transferToRight:transferToLeft;
      List<Range<Token>> toTransfer = this.endpoint2.equals(local)?transferToLeft:transferToRight;
      InetAddress preferred = SystemKeyspace.getPreferredIP(dst);
      String message = String.format("Performing streaming repair of %d ranges to %s%s", new Object[]{Integer.valueOf(transferToLeft.size()), transferToLeft.size() != transferToRight.size()?String.format(" and %d ranges from", new Object[]{Integer.valueOf(transferToRight.size())}):"", dst});
      logger.info("{} {}", this.previewKind.logPrefix(this.desc.sessionId), message);
      Tracing.traceRepair(message, new Object[0]);
      this.createStreamPlan(dst, preferred, toRequest, toTransfer).execute();
   }

   public void handleStreamEvent(StreamEvent event) {
      if (this.state == null) {
         return;
      }
      switch (event.eventType) {
         case STREAM_PREPARED: {
            StreamEvent.SessionPreparedEvent spe = (StreamEvent.SessionPreparedEvent)event;
            this.state.trace("Streaming session with {} prepared", (Object)spe.session.peer);
            break;
         }
         case STREAM_COMPLETE: {
            StreamEvent.SessionCompleteEvent sce = (StreamEvent.SessionCompleteEvent)event;
            this.state.trace("Streaming session with {} {}", (Object)sce.peer, (Object)(sce.success ? "completed successfully" : "failed"));
            break;
         }
         case FILE_PROGRESS: {
            ProgressInfo pi = ((StreamEvent.ProgressEvent)event).progress;
            Object[] arrobject = new Object[6];
            arrobject[0] = FBUtilities.prettyPrintMemory(pi.currentBytes);
            arrobject[1] = FBUtilities.prettyPrintMemory(pi.totalBytes);
            arrobject[2] = pi.currentBytes * 100L / pi.totalBytes;
            arrobject[3] = pi.direction == ProgressInfo.Direction.OUT ? "sent to" : "received from";
            arrobject[4] = pi.sessionIndex;
            arrobject[5] = pi.peer;
            this.state.trace("{}/{} ({}%) {} idx:{}{}", arrobject);
         }
      }
   }

   public void onSuccess(StreamState result) {
      String message = String.format("Sync complete using session %s between %s and %s on %s", new Object[]{this.desc.sessionId, this.endpoint1, this.endpoint2, this.desc.columnFamily});
      logger.info("[repair #{}] {}", this.desc.sessionId, message);
      Tracing.traceRepair(message, new Object[0]);
      this.set(this.stat.withSummaries(result.createSummaries()));
      this.finished();
   }

   public void onFailure(Throwable t) {
      this.setException(t);
      this.finished();
   }
}
