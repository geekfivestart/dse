package org.apache.cassandra.streaming;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableIntervalTree;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.StreamingMetrics;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.messages.CompleteMessage;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.streaming.messages.IncomingFileMessage;
import org.apache.cassandra.streaming.messages.KeepAliveMessage;
import org.apache.cassandra.streaming.messages.OutgoingFileMessage;
import org.apache.cassandra.streaming.messages.PrepareMessage;
import org.apache.cassandra.streaming.messages.ReceivedMessage;
import org.apache.cassandra.streaming.messages.SessionFailedMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.Refs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamSession {
   private static final CassandraVersion STREAM_KEEP_ALIVE_VERSION = new CassandraVersion("3.10");
   private static final Logger logger = LoggerFactory.getLogger(StreamSession.class);
   private static final DebuggableScheduledThreadPoolExecutor keepAliveExecutor = new DebuggableScheduledThreadPoolExecutor("StreamKeepAliveExecutor");
   public final InetAddress peer;
   private final int index;
   public final InetAddress connecting;
   private StreamResultFuture streamResult;
   protected final Set<StreamRequest> requests = Sets.newConcurrentHashSet();
   @VisibleForTesting
   protected final ConcurrentHashMap<TableId, StreamTransferTask> transfers = new ConcurrentHashMap();
   private final Map<TableId, StreamReceiveTask> receivers = new ConcurrentHashMap();
   private final StreamingMetrics metrics;
   private final StreamConnectionFactory factory;
   public final Map<String, Set<Range<Token>>> transferredRangesPerKeyspace = new HashMap();
   public final ConnectionHandler handler;
   private boolean canFlush = true;
   private AtomicBoolean isAborted = new AtomicBoolean(false);
   private final boolean keepSSTableLevel;
   private ScheduledFuture<?> keepAliveFuture = null;
   private final UUID pendingRepair;
   private final PreviewKind previewKind;
   private volatile StreamSession.State state;
   private volatile boolean completeSent;

   public StreamSession(InetAddress peer, InetAddress connecting, StreamConnectionFactory factory, int index, boolean keepSSTableLevel, UUID pendingRepair, PreviewKind previewKind) {
      this.state = StreamSession.State.INITIALIZED;
      this.completeSent = false;
      this.peer = peer;
      this.connecting = connecting;
      this.index = index;
      this.factory = factory;
      this.handler = new ConnectionHandler(this, previewKind.isPreview());
      this.metrics = StreamingMetrics.get(connecting);
      this.keepSSTableLevel = keepSSTableLevel;
      this.pendingRepair = pendingRepair;
      this.previewKind = previewKind;
   }

   public UUID planId() {
      return this.streamResult == null?null:this.streamResult.planId;
   }

   public int sessionIndex() {
      return this.index;
   }

   public StreamOperation streamOperation() {
      return this.streamResult == null?null:this.streamResult.streamOperation;
   }

   public boolean keepSSTableLevel() {
      return this.keepSSTableLevel;
   }

   public UUID getPendingRepair() {
      return this.pendingRepair;
   }

   public boolean isPreview() {
      return this.previewKind.isPreview();
   }

   public PreviewKind getPreviewKind() {
      return this.previewKind;
   }

   public LifecycleTransaction getTransaction(TableId tableId) {
      assert this.receivers.containsKey(tableId);

      return ((StreamReceiveTask)this.receivers.get(tableId)).getTransaction();
   }

   public void init(StreamResultFuture streamResult, boolean canFlush) {
      this.streamResult = streamResult;
      this.canFlush = canFlush;
      StreamHook.instance.reportStreamFuture(this, streamResult);
      this.scheduleKeepAliveTask();
   }

   public void start() {
      if(this.requests.isEmpty() && this.transfers.isEmpty()) {
         logger.info("[Stream #{}] Session does not have any tasks.", this.planId());
         this.closeSession(StreamSession.State.COMPLETE);
      } else {
         try {
            logger.info("[Stream #{}] Starting streaming to {}{}", new Object[]{this.planId(), this.peer, this.peer.equals(this.connecting)?"":" through " + this.connecting});
            this.handler.initiate();
            this.onInitializationComplete();
         } catch (Exception var2) {
            JVMStabilityInspector.inspectThrowable(var2);
            this.onError(var2);
         }

      }
   }

   public Socket createConnection() throws IOException {
      assert this.factory != null;

      return this.factory.createConnection(this.connecting);
   }

   public void addStreamRequest(String keyspace, Collection<Range<Token>> ranges, Collection<String> columnFamilies) {
      this.requests.add(new StreamRequest(keyspace, ranges, columnFamilies));
   }

   public synchronized void addTransferRanges(String keyspace, Collection<Range<Token>> ranges, Collection<String> columnFamilies, boolean flushTables) {
      this.failIfFinished();
      Collection<ColumnFamilyStore> stores = this.getColumnFamilyStores(keyspace, columnFamilies);
      if(this.canFlush && flushTables) {
         this.flushSSTables(stores);
      }

      List<Range<Token>> normalizedRanges = Range.normalize(ranges);
      List sections = getSSTableSectionsForRanges(normalizedRanges, stores, this.pendingRepair, this.previewKind);
      boolean var14 = false;

      try {
         var14 = true;
         this.addTransferFiles(sections);
         Object toBeUpdated = (Set)this.transferredRangesPerKeyspace.get(keyspace);
         if(toBeUpdated == null) {
            toBeUpdated = new HashSet();
         }

         ((Set)toBeUpdated).addAll(ranges);
         this.transferredRangesPerKeyspace.put(keyspace, toBeUpdated);
         var14 = false;
      } finally {
         if(var14) {
            Iterator var11 = sections.iterator();

            while(var11.hasNext()) {
               StreamSession.SSTableStreamingSections release = (StreamSession.SSTableStreamingSections)var11.next();
               release.ref.release();
            }

         }
      }

      Iterator var16 = sections.iterator();

      while(var16.hasNext()) {
         StreamSession.SSTableStreamingSections release = (StreamSession.SSTableStreamingSections)var16.next();
         release.ref.release();
      }

   }

   private void failIfFinished() {
      if(this.state().finalState) {
         throw new RuntimeException(String.format("Stream %s is finished with state %s", new Object[]{this.planId(), this.state().name()}));
      }
   }

   private Collection<ColumnFamilyStore> getColumnFamilyStores(String keyspace, Collection<String> columnFamilies) {
      Collection<ColumnFamilyStore> stores = new HashSet();
      if(columnFamilies.isEmpty()) {
         stores.addAll(Keyspace.open(keyspace).getColumnFamilyStores());
      } else {
         Iterator var4 = columnFamilies.iterator();

         while(var4.hasNext()) {
            String cf = (String)var4.next();
            stores.add(Keyspace.open(keyspace).getColumnFamilyStore(cf));
         }
      }

      return stores;
   }

   @VisibleForTesting
   public static List<StreamSession.SSTableStreamingSections> getSSTableSectionsForRanges(Collection<Range<Token>> ranges, Collection<ColumnFamilyStore> stores, UUID pendingRepair, PreviewKind previewKind) {
      Refs refs = new Refs();

      try {
         Iterator var5 = stores.iterator();

         while(var5.hasNext()) {
            ColumnFamilyStore cfStore = (ColumnFamilyStore)var5.next();
            List<Range<PartitionPosition>> keyRanges = new ArrayList(ranges.size());
            Iterator var8 = ranges.iterator();

            while(var8.hasNext()) {
               Range<Token> range = (Range)var8.next();
               keyRanges.add(Range.makeRowRange(range));
            }

            refs.addAll(cfStore.selectAndReference((view) -> {
               Set<SSTableReader> sstables = Sets.newHashSet();
               SSTableIntervalTree intervalTree = SSTableIntervalTree.build(view.select(SSTableSet.CANONICAL));
               Predicate predicate;
               if(previewKind.isPreview()) {
                  predicate = previewKind.getStreamingPredicate();
               } else if(pendingRepair == ActiveRepairService.NO_PENDING_REPAIR) {
                  predicate = Predicates.alwaysTrue();
               } else {
                  predicate = (s) -> {
                     return s.isPendingRepair() && s.getSSTableMetadata().pendingRepair.equals(pendingRepair);
                  };
               }

               Iterator var7 = keyRanges.iterator();

               while(var7.hasNext()) {
                  Range<PartitionPosition> keyRange = (Range)var7.next();
                  Iterator var9 = Iterables.filter(View.sstablesInBounds((PartitionPosition)keyRange.left, (PartitionPosition)keyRange.right, intervalTree), predicate).iterator();

                  while(var9.hasNext()) {
                     SSTableReader sstable = (SSTableReader)var9.next();
                     sstables.add(sstable);
                  }
               }

               if(logger.isDebugEnabled()) {
                  logger.debug("ViewFilter for {}/{} sstables", Integer.valueOf(sstables.size()), Integer.valueOf(Iterables.size(view.select(SSTableSet.CANONICAL))));
               }

               return sstables;
            }).refs);
         }

         List<StreamSession.SSTableStreamingSections> sections = new ArrayList(refs.size());
         Iterator var12 = refs.iterator();

         while(var12.hasNext()) {
            SSTableReader sstable = (SSTableReader)var12.next();
            sections.add(new StreamSession.SSTableStreamingSections(refs.get(sstable), sstable.getPositionsForRanges(ranges), sstable.estimatedKeysForRanges(ranges)));
         }

         return sections;
      } catch (Throwable var10) {
         refs.release();
         throw var10;
      }
   }

   public synchronized void addTransferFiles(Collection<StreamSession.SSTableStreamingSections> sstableDetails) {
      this.failIfFinished();
      Iterator iter = sstableDetails.iterator();

      while(iter.hasNext()) {
         StreamSession.SSTableStreamingSections details = (StreamSession.SSTableStreamingSections)iter.next();
         if(details.sections.isEmpty()) {
            details.ref.release();
            iter.remove();
         } else {
            TableId tableId = ((SSTableReader)details.ref.get()).metadata().id;
            StreamTransferTask task = (StreamTransferTask)this.transfers.get(tableId);
            if(task == null) {
               StreamTransferTask newTask = new StreamTransferTask(this, tableId);
               task = (StreamTransferTask)this.transfers.putIfAbsent(tableId, newTask);
               if(task == null) {
                  task = newTask;
               }
            }

            task.addTransferFile(details.ref, details.estimatedKeys, details.sections);
            iter.remove();
         }
      }

   }

   private synchronized void closeSession(StreamSession.State finalState) {
      if(this.isAborted.compareAndSet(false, true)) {
         this.state(finalState);
         if(finalState.finalState && finalState != StreamSession.State.COMPLETE) {
            Iterator var2 = Iterables.concat(this.receivers.values(), this.transfers.values()).iterator();

            while(var2.hasNext()) {
               StreamTask task = (StreamTask)var2.next();
               task.abort();
            }
         }

         if(this.keepAliveFuture != null) {
            logger.debug("[Stream #{}] Finishing keep-alive task.", this.planId());
            this.keepAliveFuture.cancel(false);
            this.keepAliveFuture = null;
         }

         this.handler.close();
         this.streamResult.handleSessionComplete(this);
      }

   }

   public void state(StreamSession.State newState) {
      this.state = newState;
   }

   public StreamSession.State state() {
      return this.state;
   }

   public boolean isSuccess() {
      return this.state == StreamSession.State.COMPLETE;
   }

   public void messageReceived(StreamMessage message) {
      switch(null.$SwitchMap$org$apache$cassandra$streaming$messages$StreamMessage$Type[message.type.ordinal()]) {
      case 1:
         PrepareMessage msg = (PrepareMessage)message;
         this.prepare(msg.requests, msg.summaries);
         break;
      case 2:
         this.receive((IncomingFileMessage)message);
         break;
      case 3:
         ReceivedMessage received = (ReceivedMessage)message;
         this.received(received.tableId, received.sequenceNumber);
         break;
      case 4:
         this.complete();
         break;
      case 5:
         this.sessionFailed();
      }

   }

   public void onInitializationComplete() {
      this.state(StreamSession.State.PREPARING);
      PrepareMessage prepare = new PrepareMessage();
      prepare.requests.addAll(this.requests);
      Iterator var2 = this.transfers.values().iterator();

      while(var2.hasNext()) {
         StreamTransferTask task = (StreamTransferTask)var2.next();
         prepare.summaries.add(task.getSummary());
      }

      this.handler.sendMessage(prepare);
      if(this.requests.isEmpty()) {
         this.startStreamingFiles();
      }

   }

   public void onError(Throwable e) {
      this.logError(e);
      if(this.handler.isOutgoingConnected()) {
         this.handler.sendMessage(new SessionFailedMessage());
      }

      this.closeSession(StreamSession.State.FAILED);
   }

   private void logError(Throwable e) {
      if(e instanceof SocketTimeoutException) {
         logger.error("[Stream #{}] Did not receive response from peer {}{} for {} secs. Is peer down? If not, maybe try increasing streaming_keep_alive_period_in_secs.", new Object[]{this.planId(), this.peer.getHostAddress(), this.peer.equals(this.connecting)?"":" through " + this.connecting.getHostAddress(), Integer.valueOf(2 * DatabaseDescriptor.getStreamingKeepAlivePeriod()), e});
      } else {
         logger.error("[Stream #{}] Streaming error occurred on session with peer {}{}", new Object[]{this.planId(), this.peer.getHostAddress(), this.peer.equals(this.connecting)?"":" through " + this.connecting.getHostAddress(), e});
      }

   }

   public void abort(String reason) {
      logger.warn("[Stream #{}] Streaming aborted: {}", this.planId(), reason);
      if(this.handler.isOutgoingConnected()) {
         this.handler.sendMessage(new SessionFailedMessage());
      }

      this.closeSession(StreamSession.State.ABORTED);
   }

   public void prepare(Collection<StreamRequest> requests, Collection<StreamSummary> summaries) {
      this.state(StreamSession.State.PREPARING);
      Iterator var3 = requests.iterator();

      while(var3.hasNext()) {
         StreamRequest request = (StreamRequest)var3.next();
         this.addTransferRanges(request.keyspace, request.ranges, request.columnFamilies, true);
      }

      var3 = summaries.iterator();

      while(var3.hasNext()) {
         StreamSummary summary = (StreamSummary)var3.next();
         this.prepareReceiving(summary);
      }

      if(!requests.isEmpty()) {
         PrepareMessage prepare = new PrepareMessage();
         Iterator var8 = this.transfers.values().iterator();

         while(var8.hasNext()) {
            StreamTransferTask task = (StreamTransferTask)var8.next();
            prepare.summaries.add(task.getSummary());
         }

         this.handler.sendMessage(prepare);
      }

      if(this.isPreview()) {
         this.completePreview();
      } else {
         if(!this.maybeCompleted()) {
            this.startStreamingFiles();
         }

      }
   }

   public void fileSent(FileMessageHeader header) {
      long headerSize = header.size();
      StreamingMetrics.totalOutgoingBytes.inc(headerSize);
      this.metrics.outgoingBytes.inc(headerSize);
      StreamTransferTask task = (StreamTransferTask)this.transfers.get(header.tableId);
      if(task != null) {
         task.scheduleTimeout(header.sequenceNumber, 12L, TimeUnit.HOURS);
      }

   }

   public void receive(IncomingFileMessage message) {
      if(this.isPreview()) {
         throw new RuntimeException("Cannot receive files for preview session");
      } else {
         long headerSize = message.header.size();
         StreamingMetrics.totalIncomingBytes.inc(headerSize);
         this.metrics.incomingBytes.inc(headerSize);
         this.handler.sendMessage(new ReceivedMessage(message.header.tableId, message.header.sequenceNumber));
         ((StreamReceiveTask)this.receivers.get(message.header.tableId)).received(message.sstable);
      }
   }

   public void progress(String filename, ProgressInfo.Direction direction, long bytes, long total) {
      ProgressInfo progress = new ProgressInfo(this.peer, this.index, filename, direction, bytes, total);
      this.streamResult.handleProgress(progress);
   }

   public void received(TableId tableId, int sequenceNumber) {
      ((StreamTransferTask)this.transfers.get(tableId)).complete(sequenceNumber);
   }

   public synchronized void complete() {
      if(this.state == StreamSession.State.WAIT_COMPLETE) {
         if(!this.completeSent) {
            this.handler.sendMessage(new CompleteMessage());
            this.completeSent = true;
         }

         this.closeSession(StreamSession.State.COMPLETE);
      } else {
         this.state(StreamSession.State.WAIT_COMPLETE);
         this.handler.closeIncoming();
      }

   }

   private synchronized void scheduleKeepAliveTask() {
      if(this.keepAliveFuture == null) {
         int keepAlivePeriod = DatabaseDescriptor.getStreamingKeepAlivePeriod();
         logger.debug("[Stream #{}] Scheduling keep-alive task with {}s period.", this.planId(), Integer.valueOf(keepAlivePeriod));
         this.keepAliveFuture = keepAliveExecutor.scheduleAtFixedRate(new StreamSession.KeepAliveTask(), 0L, (long)keepAlivePeriod, TimeUnit.SECONDS);
      }

   }

   public synchronized void sessionFailed() {
      logger.error("[Stream #{}] Remote peer {} failed stream session.", this.planId(), this.peer.getHostAddress());
      this.closeSession(StreamSession.State.FAILED);
   }

   public SessionInfo getSessionInfo() {
      List<StreamSummary> receivingSummaries = Lists.newArrayList();
      Iterator var2 = this.receivers.values().iterator();

      while(var2.hasNext()) {
         StreamTask receiver = (StreamTask)var2.next();
         receivingSummaries.add(receiver.getSummary());
      }

      List<StreamSummary> transferSummaries = Lists.newArrayList();
      Iterator var6 = this.transfers.values().iterator();

      while(var6.hasNext()) {
         StreamTask transfer = (StreamTask)var6.next();
         transferSummaries.add(transfer.getSummary());
      }

      return new SessionInfo(this.peer, this.index, this.connecting, receivingSummaries, transferSummaries, this.state);
   }

   public synchronized void taskCompleted(StreamReceiveTask completedTask) {
      this.receivers.remove(completedTask.tableId);
      this.maybeCompleted();
   }

   public synchronized void taskCompleted(StreamTransferTask completedTask) {
      this.transfers.remove(completedTask.tableId);
      this.maybeCompleted();
   }

   private void completePreview() {
      boolean var7 = false;

      try {
         var7 = true;
         this.state(StreamSession.State.WAIT_COMPLETE);
         this.closeSession(StreamSession.State.COMPLETE);
         var7 = false;
      } finally {
         if(var7) {
            Iterator var4 = Iterables.concat(this.receivers.values(), this.transfers.values()).iterator();

            while(var4.hasNext()) {
               StreamTask task = (StreamTask)var4.next();
               task.abort();
            }

         }
      }

      Iterator var1 = Iterables.concat(this.receivers.values(), this.transfers.values()).iterator();

      while(var1.hasNext()) {
         StreamTask task = (StreamTask)var1.next();
         task.abort();
      }

   }

   private boolean maybeCompleted() {
      boolean completed = this.receivers.isEmpty() && this.transfers.isEmpty();
      if(completed) {
         if(this.state == StreamSession.State.WAIT_COMPLETE) {
            if(!this.completeSent) {
               this.handler.sendMessage(new CompleteMessage());
               this.completeSent = true;
            }

            this.closeSession(StreamSession.State.COMPLETE);
         } else {
            this.handler.sendMessage(new CompleteMessage());
            this.completeSent = true;
            this.state(StreamSession.State.WAIT_COMPLETE);
            this.handler.closeOutgoing();
         }
      }

      return completed;
   }

   private void flushSSTables(Iterable<ColumnFamilyStore> stores) {
      List<Future<CommitLogPosition>> flushes = new ArrayList();
      logger.debug("[Stream #{}] Flusing {}.", this.planId(), stores);
      Iterator var3 = stores.iterator();

      while(var3.hasNext()) {
         ColumnFamilyStore cfs = (ColumnFamilyStore)var3.next();
         flushes.add(cfs.forceFlush());
      }

      FBUtilities.waitOnFutures(flushes);
      logger.debug("[Stream #{}] Finished flusing {}.", this.planId(), stores);
   }

   private synchronized void prepareReceiving(StreamSummary summary) {
      this.failIfFinished();
      if(summary.files > 0) {
         this.receivers.put(summary.tableId, new StreamReceiveTask(this, summary.tableId, summary.files, summary.totalSize));
      }

   }

   private void startStreamingFiles() {
      this.streamResult.handleSessionPrepared(this);
      this.state(StreamSession.State.STREAMING);
      Iterator var1 = this.transfers.values().iterator();

      while(var1.hasNext()) {
         StreamTransferTask task = (StreamTransferTask)var1.next();
         Collection<OutgoingFileMessage> messages = task.getFileMessages();
         if(messages.size() > 0) {
            this.handler.sendMessages(messages);
         } else {
            this.taskCompleted(task);
         }
      }

   }

   static {
      keepAliveExecutor.setRemoveOnCancelPolicy(true);
   }

   class KeepAliveTask implements Runnable {
      private KeepAliveMessage last = null;

      KeepAliveTask() {
      }

      public void run() {
         if(this.last != null && !this.last.wasSent()) {
            StreamSession.logger.trace("[Stream #{}] Skip sending keep-alive to {} (previous was not yet sent).", StreamSession.this.planId(), StreamSession.this.peer);
         } else {
            StreamSession.logger.trace("[Stream #{}] Sending keep-alive to {}.", StreamSession.this.planId(), StreamSession.this.peer);
            this.last = new KeepAliveMessage();

            try {
               StreamSession.this.handler.sendMessage(this.last);
            } catch (RuntimeException var2) {
               StreamSession.logger.debug("[Stream #{}] Could not send keep-alive message (perhaps stream session is finished?).", StreamSession.this.planId(), var2);
            }
         }

      }
   }

   public static class SSTableStreamingSections {
      public final Ref<SSTableReader> ref;
      public final List<Pair<Long, Long>> sections;
      public final long estimatedKeys;

      public SSTableStreamingSections(Ref<SSTableReader> ref, List<Pair<Long, Long>> sections, long estimatedKeys) {
         this.ref = ref;
         this.sections = sections;
         this.estimatedKeys = estimatedKeys;
      }
   }

   public static enum State {
      INITIALIZED(false),
      PREPARING(false),
      STREAMING(false),
      WAIT_COMPLETE(false),
      COMPLETE(true),
      ABORTED(true),
      FAILED(true);

      public final boolean finalState;

      private State(boolean finalState) {
         this.finalState = finalState;
      }
   }
}
