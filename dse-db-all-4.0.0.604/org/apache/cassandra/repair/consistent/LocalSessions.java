package org.apache.cassandra.repair.consistent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionStrategyManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.BoundsVersion;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OneWayRequest;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.repair.messages.FailSession;
import org.apache.cassandra.repair.messages.FinalizeCommit;
import org.apache.cassandra.repair.messages.PrepareConsistentRequest;
import org.apache.cassandra.repair.messages.PrepareConsistentResponse;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.StatusRequest;
import org.apache.cassandra.repair.messages.StatusResponse;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.flow.Flow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalSessions {
   private static final Logger logger = LoggerFactory.getLogger(LocalSessions.class);
   static final int CHECK_STATUS_TIMEOUT;
   static final int AUTO_FAIL_TIMEOUT;
   static final int AUTO_DELETE_TIMEOUT;
   public static final int CLEANUP_INTERVAL;
   private final String keyspace = "system";
   private final String table = "repairs";
   private boolean started = false;
   private volatile ImmutableMap<UUID, LocalSession> sessions = ImmutableMap.of();

   public LocalSessions() {
   }

   private static Set<TableId> uuidToTableId(Set<UUID> src) {
      return ImmutableSet.copyOf(Iterables.transform(src, TableId::fromUUID));
   }

   private static Set<UUID> tableIdToUuid(Set<TableId> src) {
      return ImmutableSet.copyOf(Iterables.transform(src, TableId::asUUID));
   }

   @VisibleForTesting
   int getNumSessions() {
      return this.sessions.size();
   }

   @VisibleForTesting
   protected InetAddress getBroadcastAddress() {
      return FBUtilities.getBroadcastAddress();
   }

   @VisibleForTesting
   protected boolean isAlive(InetAddress address) {
      return FailureDetector.instance.isAlive(address);
   }

   @VisibleForTesting
   protected boolean isNodeInitialized() {
      return StorageService.instance.isInitialized();
   }

   public List<Map<String, String>> sessionInfo(boolean all) {
      Iterable<LocalSession> currentSessions = this.sessions.values();
      if(!all) {
         currentSessions = Iterables.filter((Iterable)currentSessions, (s) -> {
            return !s.isCompleted();
         });
      }

      return Lists.newArrayList(Iterables.transform((Iterable)currentSessions, LocalSessionInfo::sessionToMap));
   }

   public void cancelSession(UUID sessionID, boolean force) {
      logger.info("Cancelling local repair session {}", sessionID);
      LocalSession session = this.getSession(sessionID);
      Preconditions.checkArgument(session != null, "Session {} does not exist", new Object[]{sessionID});
      Preconditions.checkArgument(force || session.coordinator.equals(this.getBroadcastAddress()), "Cancel session %s from it's coordinator (%s) or use --force", new Object[]{sessionID, session.coordinator});
      this.setStateAndSave(session, ConsistentSession.State.FAILED);
      UnmodifiableIterator var4 = session.participants.iterator();

      while(var4.hasNext()) {
         InetAddress participant = (InetAddress)var4.next();
         if(!participant.equals(this.getBroadcastAddress())) {
            this.send(Verbs.REPAIR.FAILED_SESSION.newRequest(participant, (Object)(new FailSession(sessionID))));
         }
      }

   }

   public void start() {
      Preconditions.checkArgument(!this.started, "LocalSessions.start can only be called once");
      Preconditions.checkArgument(this.sessions.isEmpty(), "No sessions should be added before start");
      UntypedResultSet result = QueryProcessor.executeInternalWithPaging(String.format("SELECT * FROM %s.%s", new Object[]{"system", "repairs"}), PageSize.rowsSize(1000), new Object[0]);
      Map<UUID, LocalSession> loadedSessions = new ConcurrentHashMap();
      TPCUtils.blockingAwait(result.rows().processToRxCompletable((row) -> {
         try {
            LocalSession session = this.load(row);
            loadedSessions.put(session.sessionID, session);
         } catch (NullPointerException | IllegalArgumentException var4) {
            logger.warn("Unable to load malformed repair session {}, ignoring", row.has("parent_id")?row.getUUID("parent_id"):null);
         }

      }));
      synchronized(this) {
         if(this.sessions.isEmpty() && !this.started) {
            this.sessions = ImmutableMap.copyOf(loadedSessions);
            this.started = true;
            logger.debug("Loaded {} consistent repair sessions.", Integer.valueOf(this.sessions.size()));
         }

      }
   }

   public boolean isStarted() {
      return this.started;
   }

   private static boolean shouldCheckStatus(LocalSession session, int now) {
      return !session.isCompleted() && now > session.getLastUpdate() + CHECK_STATUS_TIMEOUT;
   }

   private static boolean shouldFail(LocalSession session, int now) {
      return !session.isCompleted() && now > session.getLastUpdate() + AUTO_FAIL_TIMEOUT;
   }

   private static boolean shouldDelete(LocalSession session, int now) {
      return session.isCompleted() && now > session.getLastUpdate() + AUTO_DELETE_TIMEOUT;
   }

   public void cleanup() {
      logger.debug("Running LocalSessions.cleanup");
      if(!this.isNodeInitialized()) {
         logger.trace("node not initialized, aborting local session cleanup");
      } else {
         Set<LocalSession> currentSessions = new HashSet(this.sessions.values());
         Iterator var2 = currentSessions.iterator();

         while(var2.hasNext()) {
            LocalSession session = (LocalSession)var2.next();
            synchronized(session) {
               int now = FBUtilities.nowInSeconds();
               logger.debug("Cleaning up consistent repair session at {}: {}", Integer.valueOf(now), session);
               if(shouldFail(session, now)) {
                  logger.warn("Auto failing timed out repair session {}", session);
                  this.failSession(session.sessionID);
               } else if(shouldDelete(session, now)) {
                  if(!this.sessionHasData(session)) {
                     logger.debug("Auto deleting repair session {}", session);
                     this.deleteSession(session.sessionID);
                  } else {
                     logger.warn("Skipping delete of LocalSession {} because it still contains sstables", session.sessionID);
                  }
               } else if(shouldCheckStatus(session, now)) {
                  this.sendStatusRequest(session);
               }
            }
         }

      }
   }

   private static ByteBuffer serializeRange(Range<Token> range) {
      int size = Token.serializer.serializedSize((Token)range.left, BoundsVersion.OSS_30);
      size += Token.serializer.serializedSize((Token)range.right, BoundsVersion.OSS_30);

      try {
         DataOutputBuffer buffer = new DataOutputBuffer(size);
         Throwable var3 = null;

         ByteBuffer var4;
         try {
            Token.serializer.serialize((Token)((Token)range.left), buffer, (BoundsVersion)BoundsVersion.OSS_30);
            Token.serializer.serialize((Token)((Token)range.right), buffer, (BoundsVersion)BoundsVersion.OSS_30);
            var4 = buffer.buffer();
         } catch (Throwable var14) {
            var3 = var14;
            throw var14;
         } finally {
            if(buffer != null) {
               if(var3 != null) {
                  try {
                     buffer.close();
                  } catch (Throwable var13) {
                     var3.addSuppressed(var13);
                  }
               } else {
                  buffer.close();
               }
            }

         }

         return var4;
      } catch (IOException var16) {
         throw new RuntimeException(var16);
      }
   }

   private static Set<ByteBuffer> serializeRanges(Set<Range<Token>> ranges) {
      Set<ByteBuffer> buffers = new HashSet(ranges.size());
      ranges.forEach((r) -> {
         buffers.add(serializeRange(r));
      });
      return buffers;
   }

   private static Range<Token> deserializeRange(ByteBuffer bb) {
      try {
         DataInputBuffer in = new DataInputBuffer(bb, false);
         Throwable var2 = null;

         Range var6;
         try {
            IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
            Token left = Token.serializer.deserialize(in, partitioner, (BoundsVersion)BoundsVersion.OSS_30);
            Token right = Token.serializer.deserialize(in, partitioner, (BoundsVersion)BoundsVersion.OSS_30);
            var6 = new Range(left, right);
         } catch (Throwable var16) {
            var2 = var16;
            throw var16;
         } finally {
            if(in != null) {
               if(var2 != null) {
                  try {
                     in.close();
                  } catch (Throwable var15) {
                     var2.addSuppressed(var15);
                  }
               } else {
                  in.close();
               }
            }

         }

         return var6;
      } catch (IOException var18) {
         throw new RuntimeException(var18);
      }
   }

   private static Set<Range<Token>> deserializeRanges(Set<ByteBuffer> buffers) {
      Set<Range<Token>> ranges = new HashSet(buffers.size());
      buffers.forEach((bb) -> {
         ranges.add(deserializeRange(bb));
      });
      return ranges;
   }

   @VisibleForTesting
   void save(LocalSession session) {
      String query = "INSERT INTO %s.%s (parent_id, started_at, last_update, repaired_at, state, coordinator, participants, ranges, cfids) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
      logger.debug("Saving session: " + session);
      QueryProcessor.executeInternal(String.format(query, new Object[]{"system", "repairs"}), new Object[]{session.sessionID, Date.from(Instant.ofEpochSecond((long)session.startedAt)), Date.from(Instant.ofEpochSecond((long)session.getLastUpdate())), Date.from(Instant.ofEpochMilli(session.repairedAt)), Integer.valueOf(session.getState().ordinal()), session.coordinator, session.participants, serializeRanges(session.ranges), tableIdToUuid(session.tableIds)});

      try {
         CommitLog.instance.sync();
      } catch (IOException var4) {
         logger.warn("Failed to sync commit log, this could cause repair inconsistencies.", var4);
      }

   }

   private static int dateToSeconds(Date d) {
      return Ints.checkedCast(TimeUnit.MILLISECONDS.toSeconds(d.getTime()));
   }

   private LocalSession load(UntypedResultSet.Row row) {
      LocalSession.Builder builder = LocalSession.builder();
      builder.withState(ConsistentSession.State.valueOf(row.getInt("state")));
      builder.withSessionID(row.getUUID("parent_id"));
      builder.withCoordinator(row.getInetAddress("coordinator"));
      builder.withTableIds(uuidToTableId(row.getSet("cfids", UUIDType.instance)));
      builder.withRepairedAt(row.getTimestamp("repaired_at").getTime());
      builder.withRanges(deserializeRanges(row.getSet("ranges", BytesType.instance)));
      builder.withParticipants(row.getSet("participants", InetAddressType.instance));
      builder.withStartedAt(dateToSeconds(row.getTimestamp("started_at")));
      builder.withLastUpdate(dateToSeconds(row.getTimestamp("last_update")));
      LocalSession session = this.buildSession(builder);
      logger.debug("Loaded consistent repair session: {}", session);
      return session;
   }

   private void deleteRow(UUID sessionID) {
      String query = "DELETE FROM %s.%s WHERE parent_id=?";
      QueryProcessor.executeInternal(String.format(query, new Object[]{"system", "repairs"}), new Object[]{sessionID});
   }

   private void syncTable() {
      TableId tid = Schema.instance.getTableMetadata("system", "repairs").id;
      ColumnFamilyStore cfm = Schema.instance.getColumnFamilyStoreInstance(tid);
      cfm.forceBlockingFlush();
   }

   @VisibleForTesting
   public LocalSession loadUnsafe(UUID sessionId) {
      String query = "SELECT * FROM %s.%s WHERE parent_id=?";
      UntypedResultSet result = QueryProcessor.executeInternal(String.format(query, new Object[]{"system", "repairs"}), new Object[]{sessionId});
      if(result.isEmpty()) {
         return null;
      } else {
         UntypedResultSet.Row row = result.one();
         return this.load(row);
      }
   }

   @VisibleForTesting
   public LocalSession buildSession(LocalSession.Builder builder) {
      return new LocalSession(builder);
   }

   @VisibleForTesting
   public LocalSession getSession(UUID sessionID) {
      return (LocalSession)this.sessions.get(sessionID);
   }

   @VisibleForTesting
   public synchronized void putSessionUnsafe(LocalSession session) {
      this.putSession(session);
      this.save(session);
   }

   private synchronized void putSession(LocalSession session) {
      Preconditions.checkArgument(!this.sessions.containsKey(session.sessionID), "LocalSession {} already exists", new Object[]{session.sessionID});
      Preconditions.checkArgument(this.started, "sessions cannot be added before LocalSessions is started");
      this.sessions = ImmutableMap.builder().putAll(this.sessions).put(session.sessionID, session).build();
   }

   private synchronized void removeSession(UUID sessionID) {
      Preconditions.checkArgument(sessionID != null);
      Map<UUID, LocalSession> temp = new HashMap(this.sessions);
      temp.remove(sessionID);
      this.sessions = ImmutableMap.copyOf(temp);
   }

   @VisibleForTesting
   LocalSession createSessionUnsafe(UUID sessionId, ActiveRepairService.ParentRepairSession prs, Set<InetAddress> peers) {
      LocalSession.Builder builder = LocalSession.builder();
      builder.withState(ConsistentSession.State.PREPARING);
      builder.withSessionID(sessionId);
      builder.withCoordinator(prs.coordinator);
      builder.withTableIds(prs.getTableIds());
      builder.withRepairedAt(prs.repairedAt);
      builder.withRanges(prs.getRanges());
      builder.withParticipants(peers);
      int now = FBUtilities.nowInSeconds();
      builder.withStartedAt(now);
      builder.withLastUpdate(now);
      return this.buildSession(builder);
   }

   protected ActiveRepairService.ParentRepairSession getParentRepairSession(UUID sessionID) {
      return ActiveRepairService.instance.getParentRepairSession(sessionID);
   }

   protected void send(OneWayRequest<? extends RepairMessage<?>> request) {
      logger.trace("sending {} to {}", request.payload(), request.to());
      MessagingService.instance().send(request);
   }

   @VisibleForTesting
   protected <REQ extends RepairMessage, RES extends RepairMessage> void send(Request<REQ, RES> request, MessageCallback<RES> callback) {
      logger.trace("sending {} to {}", request.payload(), request.to());
      MessagingService.instance().send(request, callback);
   }

   private void setStateAndSave(LocalSession session, ConsistentSession.State state) {
      boolean wasCompleted;
      synchronized(session) {
         Preconditions.checkArgument(session.getState().canTransitionTo(state), "Invalid state transition %s -> %s", new Object[]{session.getState(), state});
         logger.debug("Changing LocalSession state from {} -> {} for {}", new Object[]{session.getState(), state, session.sessionID});
         wasCompleted = session.isCompleted();
         session.setState(state);
         session.setLastUpdate();
         this.save(session);
      }

      if(session.isCompleted() && !wasCompleted) {
         this.sessionCompleted(session);
      }

   }

   private void failPrepare(UUID session, InetAddress coordinator, ExecutorService executor) {
      this.failSession(session);
      this.send(Verbs.REPAIR.CONSISTENT_RESPONSE.newRequest(coordinator, (Object)(new PrepareConsistentResponse(session, this.getBroadcastAddress(), false))));
      executor.shutdown();
   }

   public void failSession(UUID sessionID) {
      LocalSession session = this.getSession(sessionID);
      if(session != null && session.getState() != ConsistentSession.State.FINALIZED) {
         logger.info("Failing local repair session {}", sessionID);
         this.setStateAndSave(session, ConsistentSession.State.FAILED);
      }

   }

   public synchronized void deleteSession(UUID sessionID) {
      logger.debug("Deleting local repair session {}", sessionID);
      LocalSession session = this.getSession(sessionID);
      Preconditions.checkArgument(session.isCompleted(), "Cannot delete incomplete sessions");
      this.deleteRow(sessionID);
      this.removeSession(sessionID);
   }

   @VisibleForTesting
   ListenableFuture<Boolean> resolveSessions(LocalSession newSession, ExecutorService executor) {
      ListenableFutureTask<Boolean> task = ListenableFutureTask.create(new LocalSessionsResolver(newSession, this.sessions.values(), (sessionAndState) -> {
         this.setStateAndSave((LocalSession)sessionAndState.left, (ConsistentSession.State)sessionAndState.right);
      }));
      executor.submit(task);
      return task;
   }

   @VisibleForTesting
   ListenableFuture submitPendingAntiCompaction(LocalSession session, ExecutorService executor) {
      PendingAntiCompaction pac = new PendingAntiCompaction(session.sessionID, session.ranges, executor);
      return pac.run();
   }

   public void handlePrepareMessage(InetAddress from, PrepareConsistentRequest request) {
      logger.trace("received {} from {}", request, from);
      final UUID sessionID = request.sessionID;
      final InetAddress coordinator = request.coordinator;
      Set peers = request.participants;

      ActiveRepairService.ParentRepairSession parentSession;
      try {
         parentSession = this.getParentRepairSession(sessionID);
      } catch (Throwable var10) {
         logger.trace("Error retrieving ParentRepairSession for session {}, responding with failure", sessionID);
         this.send(Verbs.REPAIR.FAILED_SESSION.newRequest(coordinator, (Object)(new FailSession(sessionID))));
         return;
      }

      final LocalSession session = this.createSessionUnsafe(sessionID, parentSession, peers);
      this.putSessionUnsafe(session);
      final ExecutorService executor = this.getPrepareExecutor(parentSession);
      ListenableFuture<Boolean> resolveFuture = this.resolveSessions(session, executor);
      Futures.addCallback(resolveFuture, new FutureCallback<Boolean>() {
         public void onSuccess(Boolean resolved) {
            if(resolved.booleanValue()) {
               LocalSessions.logger.info("Beginning local incremental repair session {}", session);
               ListenableFuture pendingAntiCompaction = LocalSessions.this.submitPendingAntiCompaction(session, executor);
               Futures.addCallback(pendingAntiCompaction, new FutureCallback() {
                  public void onSuccess(@Nullable Object result) {
                     LocalSessions.logger.debug("Prepare phase for incremental repair session {} completed", sessionID);
                     LocalSessions.this.setStateAndSave(session, ConsistentSession.State.PREPARED);
                     LocalSessions.this.send(Verbs.REPAIR.CONSISTENT_RESPONSE.newRequest(coordinator, (Object)(new PrepareConsistentResponse(sessionID, LocalSessions.this.getBroadcastAddress(), true))));
                     executor.shutdown();
                  }

                  public void onFailure(Throwable t) {
                     LocalSessions.logger.error(String.format("Prepare phase for incremental repair session %s failed", new Object[]{sessionID}), t);
                     LocalSessions.this.failPrepare(session.sessionID, coordinator, executor);
                  }
               });
            } else {
               LocalSessions.logger.error(String.format("Prepare phase for incremental repair session %s aborted due to session resolution failure.", new Object[]{sessionID}));
               LocalSessions.this.failPrepare(session.sessionID, coordinator, executor);
            }

         }

         public void onFailure(Throwable t) {
            LocalSessions.logger.error("Prepare phase for incremental repair session {} failed", sessionID, t);
            if(t instanceof PendingAntiCompaction.SSTableAcquisitionException) {
               LocalSessions.logger.warn("Prepare phase for incremental repair session {} was unable to acquire exclusive access to the neccesary sstables. This is usually caused by running multiple incremental repairs on nodes that share token ranges", sessionID);
            } else {
               LocalSessions.logger.error("Prepare phase for incremental repair session {} failed", sessionID, t);
            }

            LocalSessions.this.failPrepare(session.sessionID, coordinator, executor);
         }
      });
   }

   @VisibleForTesting
   protected ExecutorService getPrepareExecutor(ActiveRepairService.ParentRepairSession parentSession) {
      return Executors.newFixedThreadPool(parentSession.getColumnFamilyStores().size(), (new ThreadFactoryBuilder()).setDaemon(true).setNameFormat("Inc-Repair-prepare-executor-%d").build());
   }

   public void maybeSetRepairing(UUID sessionID) {
      LocalSession session = this.getSession(sessionID);
      if(session != null && session.getState() != ConsistentSession.State.REPAIRING) {
         logger.debug("Setting local incremental repair session {} to REPAIRING", session);
         this.setStateAndSave(session, ConsistentSession.State.REPAIRING);
      }

   }

   @VisibleForTesting
   public synchronized void sessionCompleted(LocalSession session) {
      logger.info("Completing local repair session {}", session.sessionID);
      UnmodifiableIterator var2 = session.tableIds.iterator();

      while(var2.hasNext()) {
         TableId tid = (TableId)var2.next();
         ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(tid);
         if(cfs != null) {
            logger.info("Running pending repair sstables resolution for local repair session {} and table {}.{}", new Object[]{session.sessionID, cfs.keyspace, cfs.name});
            cfs.runWithCompactionsDisabled(() -> {
               CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();
               strategy.getPendingRepairTasks(session.sessionID).stream().forEach((task) -> {
                  task.run();
               });
               return null;
            }, OperationType.COMPACTIONS_ONLY, (s) -> {
               return session.sessionID.equals(s.getPendingRepair());
            }, false);
         }
      }

   }

   public void handleFinalizeCommitMessage(InetAddress from, FinalizeCommit commit) {
      logger.trace("received {} from {}", commit, from);
      UUID sessionID = commit.sessionID;
      LocalSession session = this.getSession(sessionID);
      if(session == null) {
         logger.warn("Ignoring FinalizeCommit message for unknown repair session {}", sessionID);
      } else {
         this.setStateAndSave(session, ConsistentSession.State.FINALIZED);
         logger.info("Finalized local repair session {}", sessionID);
      }
   }

   public void handleFailSessionMessage(InetAddress from, FailSession msg) {
      logger.trace("received {} from {}", msg, from);
      this.failSession(msg.sessionID);
   }

   public void sendStatusRequest(LocalSession session) {
      logger.debug("Attempting to learn the outcome of unfinished local incremental repair session {}", session.sessionID);
      StatusRequest request = new StatusRequest(session.sessionID);
      LocalSessions.LocalStatusResponseCallback callback = new LocalSessions.LocalStatusResponseCallback(null);
      UnmodifiableIterator var4 = session.participants.iterator();

      while(var4.hasNext()) {
         InetAddress participant = (InetAddress)var4.next();
         if(!this.getBroadcastAddress().equals(participant) && this.isAlive(participant)) {
            this.send(Verbs.REPAIR.STATUS_REQUEST.newRequest(participant, request), callback);
         }
      }

   }

   public StatusResponse handleStatusRequest(InetAddress from, StatusRequest request) {
      logger.trace("received {} from {}", request, from);
      UUID sessionID = request.sessionID;
      LocalSession session = this.getSession(sessionID);
      if(session == null) {
         logger.warn("Received status response message for unknown session {}", sessionID);
         return new StatusResponse(sessionID, ConsistentSession.State.UNKNOWN);
      } else {
         boolean running = ActiveRepairService.instance.hasParentRepairSession(sessionID);
         logger.debug("Responding to status response message for incremental repair session {}{} with local state {}", new Object[]{sessionID, running?" running locally":" ", session.getState()});
         return new StatusResponse(sessionID, session.getState(), running);
      }
   }

   @VisibleForTesting
   public boolean handleStatusResponse(InetAddress from, StatusResponse response) {
      logger.trace("received {} from {}", response, from);
      UUID sessionID = response.sessionID;
      LocalSession session = this.getSession(sessionID);
      if(session == null) {
         logger.warn("Received StatusResponse message for unknown repair session {}", sessionID);
         return false;
      } else if(response.state != ConsistentSession.State.FINALIZED && response.state != ConsistentSession.State.FAILED) {
         logger.debug("Received StatusResponse for repair session {} with state {}, which is not actionable. Doing nothing.", sessionID, response.state);
         return false;
      } else {
         this.setStateAndSave(session, response.state);
         logger.info("Unfinished local incremental repair session {} set to state {}", sessionID, response.state);
         return true;
      }
   }

   public boolean isSessionInProgress(UUID sessionID) {
      LocalSession session = this.getSession(sessionID);
      return session != null && session.getState() != ConsistentSession.State.FINALIZED && session.getState() != ConsistentSession.State.FAILED;
   }

   @VisibleForTesting
   protected boolean sessionHasData(LocalSession session) {
      java.util.function.Predicate<TableId> predicate = (tid) -> {
         ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(tid);
         return cfs != null && cfs.getCompactionStrategyManager().hasDataForPendingRepair(session.sessionID);
      };
      ImmutableSet var10000 = session.tableIds;
      predicate.getClass();
      return Iterables.any(var10000, predicate::test);
   }

   public long getFinalSessionRepairedAt(UUID sessionID) {
      LocalSession session = this.getSession(sessionID);
      if(session != null && session.getState() != ConsistentSession.State.FAILED) {
         if(session.getState() == ConsistentSession.State.FINALIZED) {
            return session.repairedAt;
         } else {
            throw new IllegalStateException("Cannot get final repaired at value for in progress session: " + session);
         }
      } else {
         return 0L;
      }
   }

   static {
      CHECK_STATUS_TIMEOUT = Integer.getInteger("cassandra.repair_status_check_timeout_seconds", Ints.checkedCast(TimeUnit.HOURS.toSeconds(1L))).intValue();
      AUTO_FAIL_TIMEOUT = Integer.getInteger("cassandra.repair_fail_timeout_seconds", Ints.checkedCast(TimeUnit.DAYS.toSeconds(1L))).intValue();
      AUTO_DELETE_TIMEOUT = Integer.getInteger("cassandra.repair_delete_timeout_seconds", Ints.checkedCast(TimeUnit.DAYS.toSeconds(1L))).intValue();
      CLEANUP_INTERVAL = Integer.getInteger("cassandra.repair_cleanup_interval_seconds", Ints.checkedCast(TimeUnit.MINUTES.toSeconds(10L))).intValue();
   }

   private class LocalStatusResponseCallback implements MessageCallback<StatusResponse> {
      private boolean completed;

      private LocalStatusResponseCallback() {
      }

      public synchronized void onResponse(Response<StatusResponse> response) {
         if(!this.completed) {
            this.completed = LocalSessions.this.handleStatusResponse(response.from(), (StatusResponse)response.payload());
         }

      }

      public void onFailure(FailureResponse<StatusResponse> failure) {
      }
   }
}
