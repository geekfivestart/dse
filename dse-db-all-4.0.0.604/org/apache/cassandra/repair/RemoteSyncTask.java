package org.apache.cassandra.repair;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RepairException;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.repair.messages.SyncRequest;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.SessionSummary;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.RangeHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteSyncTask extends SyncTask {
   private static final Logger logger = LoggerFactory.getLogger(RemoteSyncTask.class);
   private final RepairSession session;

   public RemoteSyncTask(RepairJobDesc desc, TreeResponse r1, TreeResponse r2, RepairSession session, Executor taskExecutor, SyncTask next, Map<InetAddress, Set<RangeHash>> receivedRangeCache, PreviewKind previewKind) {
      super(desc, r1, r2, taskExecutor, next, receivedRangeCache, previewKind);
      this.session = session;
   }

   protected void startSync(List<Range<Token>> transferToLeft, List<Range<Token>> transferToRight) {
      this.session.waitForSync(Pair.create(this.desc, new NodePair(this.endpoint1, this.endpoint2)), this);
      InetAddress local = FBUtilities.getBroadcastAddress();
      List<Range<Token>> ranges = new ArrayList(transferToLeft.size() + transferToRight.size() + 1);
      ranges.addAll(transferToLeft);
      ranges.add(StreamingRepairTask.RANGE_SEPARATOR);
      ranges.addAll(transferToRight);
      SyncRequest request = new SyncRequest(this.desc, local, this.endpoint1, this.endpoint2, ranges, this.previewKind);
      String message = String.format("Forwarding streaming repair of %d ranges to %s%s (to be streamed with %s)", new Object[]{Integer.valueOf(transferToLeft.size()), request.src, transferToLeft.size() != transferToRight.size()?String.format(" and %d ranges from", new Object[]{Integer.valueOf(transferToRight.size())}):"", request.dst});
      logger.info("{} {}", this.previewKind.logPrefix(this.desc.sessionId), message);
      Tracing.traceRepair(message, new Object[0]);
      MessagingService.instance().send(Verbs.REPAIR.SYNC_REQUEST.newRequest(request.src, request));
   }

   public void syncComplete(boolean success, List<SessionSummary> summaries) {
      if(success) {
         this.set(this.stat.withSummaries(summaries));
      } else {
         this.setException(new RepairException(this.desc, this.previewKind, String.format("Sync failed between %s and %s", new Object[]{this.endpoint1, this.endpoint2})));
      }

      this.finished();
   }
}
