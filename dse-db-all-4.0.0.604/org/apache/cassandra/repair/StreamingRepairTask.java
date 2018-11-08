package org.apache.cassandra.repair;

import com.google.common.annotations.VisibleForTesting;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.repair.messages.SyncComplete;
import org.apache.cassandra.repair.messages.SyncRequest;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingRepairTask implements Runnable, StreamEventHandler {
   private static final Logger logger = LoggerFactory.getLogger(StreamingRepairTask.class);
   public static final String REPAIR_STREAM_PLAN_DESCRIPTION = "Repair";
   private final RepairJobDesc desc;
   private final SyncRequest request;
   private final UUID pendingRepair;
   private final PreviewKind previewKind;
   public static final Range<Token> RANGE_SEPARATOR = new Range(MessagingService.globalPartitioner().getMinimumToken(), MessagingService.globalPartitioner().getMinimumToken());

   public StreamingRepairTask(RepairJobDesc desc, SyncRequest request, UUID pendingRepair, PreviewKind previewKind) {
      this.desc = desc;
      this.request = request;
      this.pendingRepair = pendingRepair;
      this.previewKind = previewKind;
   }

   public void run() {
      InetAddress dest = this.request.dst;
      InetAddress preferred = SystemKeyspace.getPreferredIP(dest);
      logger.info("[streaming task #{}] Performing streaming repair of {} ranges with {}", new Object[]{this.desc.sessionId, Integer.valueOf(this.request.ranges.size()), this.request.dst});
      this.createStreamPlan(dest, preferred).execute();
   }

   @VisibleForTesting
   StreamPlan createStreamPlan(InetAddress dest, InetAddress preferred) {
      List<Range<Token>> toRequest = new LinkedList();
      List<Range<Token>> toTransfer = new LinkedList();
      boolean head = true;
      Iterator var6 = this.request.ranges.iterator();

      while(var6.hasNext()) {
         Range<Token> range = (Range)var6.next();
         if(range.equals(RANGE_SEPARATOR)) {
            head = false;
         } else if(head) {
            toRequest.add(range);
         } else {
            toTransfer.add(range);
         }
      }

      logger.info(String.format("[streaming task #%s] Performing streaming repair of %d ranges to and %d ranges from %s.", new Object[]{this.desc.sessionId, Integer.valueOf(toTransfer.size()), Integer.valueOf(toRequest.size()), this.request.dst}));
      return (new StreamPlan(StreamOperation.REPAIR, 1, false, false, this.pendingRepair, this.previewKind)).listeners(this, new StreamEventHandler[0]).flushBeforeTransfer(false).requestRanges(dest, preferred, this.desc.keyspace, toRequest, new String[]{this.desc.columnFamily}).transferRanges(dest, preferred, this.desc.keyspace, toTransfer, new String[]{this.desc.columnFamily});
   }

   public void handleStreamEvent(StreamEvent event) {
   }

   public void onSuccess(StreamState state) {
      logger.info("{} streaming task succeed, returning response to {}", this.previewKind.logPrefix(this.desc.sessionId), this.request.initiator);
      MessagingService.instance().send(Verbs.REPAIR.SYNC_COMPLETE.newRequest(this.request.initiator, (Object)(new SyncComplete(this.desc, this.request.src, this.request.dst, true, state.createSummaries()))));
   }

   public void onFailure(Throwable t) {
      MessagingService.instance().send(Verbs.REPAIR.SYNC_COMPLETE.newRequest(this.request.initiator, (Object)(new SyncComplete(this.desc, this.request.src, this.request.dst, false, Collections.emptyList()))));
   }
}
