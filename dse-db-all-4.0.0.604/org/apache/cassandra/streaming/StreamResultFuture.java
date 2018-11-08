package org.apache.cassandra.streaming;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import org.apache.cassandra.net.IncomingStreamingConnection;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StreamResultFuture extends AbstractFuture<StreamState> {
   private static final Logger logger = LoggerFactory.getLogger(StreamResultFuture.class);
   public final UUID planId;
   public final StreamOperation streamOperation;
   private final StreamCoordinator coordinator;
   private final Collection<StreamEventHandler> eventListeners;

   private StreamResultFuture(UUID planId, StreamOperation streamOperation, StreamCoordinator coordinator) {
      this.eventListeners = new ConcurrentLinkedQueue();
      this.planId = planId;
      this.streamOperation = streamOperation;
      this.coordinator = coordinator;
      if(!coordinator.isReceiving() && !coordinator.hasActiveSessions()) {
         this.set(this.getCurrentState());
      }

   }

   private StreamResultFuture(UUID planId, StreamOperation streamOperation, boolean keepSSTableLevels, UUID pendingRepair, PreviewKind previewKind) {
      this(planId, streamOperation, new StreamCoordinator(0, keepSSTableLevels, new DefaultConnectionFactory(), false, pendingRepair, previewKind));
   }

   static StreamResultFuture init(UUID planId, StreamOperation streamOperation, Collection<StreamEventHandler> listeners, StreamCoordinator coordinator) {
      StreamResultFuture future = createAndRegister(planId, streamOperation, coordinator);
      Iterator var5;
      if(listeners != null) {
         var5 = listeners.iterator();

         while(var5.hasNext()) {
            StreamEventHandler listener = (StreamEventHandler)var5.next();
            future.addEventListener(listener);
         }
      }

      logger.info("[Stream #{}] Executing streaming plan for {}", planId, streamOperation.getDescription());
      var5 = coordinator.getAllStreamSessions().iterator();

      while(var5.hasNext()) {
         StreamSession session = (StreamSession)var5.next();
         session.init(future, !future.isRepairSession());
      }

      coordinator.connect(future);
      return future;
   }

   public static synchronized StreamResultFuture initReceivingSide(int sessionIndex, UUID planId, StreamOperation streamOperation, InetAddress from, IncomingStreamingConnection connection, boolean isForOutgoing, StreamMessage.StreamVersion version, boolean keepSSTableLevel, UUID pendingRepair, PreviewKind previewKind) throws IOException {
      StreamResultFuture future = StreamManager.instance.getReceivingStream(planId);
      if(future == null) {
         logger.info("[Stream #{} ID#{}] Creating new streaming plan for {}", new Object[]{planId, Integer.valueOf(sessionIndex), streamOperation.getDescription()});
         future = new StreamResultFuture(planId, streamOperation, keepSSTableLevel, pendingRepair, previewKind);
         StreamManager.instance.registerReceiving(future);
      }

      future.attachConnection(from, sessionIndex, connection, isForOutgoing, version);
      logger.info("[Stream #{}, ID#{}] Received streaming plan for {}", new Object[]{planId, Integer.valueOf(sessionIndex), streamOperation.getDescription()});
      return future;
   }

   private static StreamResultFuture createAndRegister(UUID planId, StreamOperation streamOperation, StreamCoordinator coordinator) {
      StreamResultFuture future = new StreamResultFuture(planId, streamOperation, coordinator);
      StreamManager.instance.register(future);
      return future;
   }

   private void attachConnection(InetAddress from, int sessionIndex, IncomingStreamingConnection connection, boolean isForOutgoing, StreamMessage.StreamVersion version) throws IOException {
      StreamSession session = this.coordinator.getOrCreateSessionById(from, sessionIndex, connection.socket.getInetAddress());
      session.init(this, !this.isRepairSession());
      session.handler.initiateOnReceivingSide(connection, isForOutgoing, version);
   }

   private boolean isRepairSession() {
      return "Repair".equals(this.streamOperation.getDescription());
   }

   public void addEventListener(StreamEventHandler listener) {
      Futures.addCallback(this, listener);
      this.eventListeners.add(listener);
   }

   public StreamState getCurrentState() {
      return new StreamState(this.planId, this.streamOperation, this.coordinator.getAllSessionInfo());
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         StreamResultFuture that = (StreamResultFuture)o;
         return this.planId.equals(that.planId);
      } else {
         return false;
      }
   }

   public int hashCode() {
      return this.planId.hashCode();
   }

   void handleSessionPrepared(StreamSession session) {
      SessionInfo sessionInfo = session.getSessionInfo();
      logger.info("[Stream #{} ID#{}] Prepare completed. Receiving {} files({}), sending {} files({})", new Object[]{session.planId(), Integer.valueOf(session.sessionIndex()), Long.valueOf(sessionInfo.getTotalFilesToReceive()), FBUtilities.prettyPrintMemory(sessionInfo.getTotalSizeToReceive()), Long.valueOf(sessionInfo.getTotalFilesToSend()), FBUtilities.prettyPrintMemory(sessionInfo.getTotalSizeToSend())});
      StreamEvent.SessionPreparedEvent event = new StreamEvent.SessionPreparedEvent(this.planId, sessionInfo);
      this.coordinator.addSessionInfo(sessionInfo);
      this.fireStreamEvent(event);
   }

   void handleSessionComplete(StreamSession session) {
      logger.info("[Stream #{}] Session with {} is complete", session.planId(), session.peer);
      this.fireStreamEvent(new StreamEvent.SessionCompleteEvent(session));
      SessionInfo sessionInfo = session.getSessionInfo();
      this.coordinator.addSessionInfo(sessionInfo);
      this.maybeComplete();
   }

   public void handleProgress(ProgressInfo progress) {
      this.coordinator.updateProgress(progress);
      this.fireStreamEvent(new StreamEvent.ProgressEvent(this.planId, progress));
   }

   synchronized void fireStreamEvent(StreamEvent event) {
      Iterator var2 = this.eventListeners.iterator();

      while(var2.hasNext()) {
         StreamEventHandler listener = (StreamEventHandler)var2.next();
         listener.handleStreamEvent(event);
      }

   }

   private synchronized void maybeComplete() {
      if(!this.coordinator.hasActiveSessions()) {
         StreamState finalState = this.getCurrentState();
         if(finalState.hasFailedSession()) {
            logger.warn("[Stream #{}] Stream failed", this.planId);
            this.setException(new StreamException(finalState, "Stream failed"));
         } else if(finalState.hasAbortedSession()) {
            logger.warn("[Stream #{}] Stream aborted", this.planId);
            this.setException(new StreamException(finalState, "Stream aborted"));
         } else {
            logger.info("[Stream #{}] All sessions completed", this.planId);
            this.set(finalState);
         }
      }

   }

   public void abort(String reason) {
      ArrayList<StreamSession> sessions = new ArrayList(this.coordinator.getAllStreamSessions());
      logger.warn("Aborting streaming for {} active sessions.", Integer.valueOf(sessions.size()));
      sessions.forEach((s) -> {
         s.abort(reason);
      });
   }
}
