package org.apache.cassandra.repair.consistent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.repair.messages.PrepareConsistentResponse;
import org.apache.cassandra.service.ActiveRepairService;

public class CoordinatorSessions {
   private final Map<UUID, CoordinatorSession> sessions;
   private final IFailureDetector failureDetector;
   private final Gossiper gossiper;

   @VisibleForTesting
   public CoordinatorSessions() {
      this((IFailureDetector)null, (Gossiper)null);
   }

   public CoordinatorSessions(IFailureDetector failureDetector, Gossiper gossiper) {
      this.sessions = new HashMap();
      this.failureDetector = failureDetector;
      this.gossiper = gossiper;
   }

   protected CoordinatorSession buildSession(CoordinatorSession.Builder builder) {
      return new CoordinatorSession(builder);
   }

   public synchronized CoordinatorSession registerSession(UUID sessionId, Set<InetAddress> participants) {
      Preconditions.checkArgument(!this.sessions.containsKey(sessionId), "A coordinator already exists for session %s", new Object[]{sessionId});
      ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(sessionId);
      CoordinatorSession.Builder builder = CoordinatorSession.builder();
      builder.withState(ConsistentSession.State.PREPARING);
      builder.withSessionID(sessionId);
      builder.withCoordinator(prs.coordinator);
      builder.withTableIds(prs.getTableIds());
      builder.withRepairedAt(prs.repairedAt);
      builder.withRanges(prs.getRanges());
      builder.withParticipants(participants);
      CoordinatorSession session = this.buildSession(builder);
      this.sessions.put(session.sessionID, session);
      if(this.failureDetector != null && this.gossiper != null) {
         session.setOnCompleteCallback((s) -> {
            if(this.failureDetector != null && this.gossiper != null) {
               this.failureDetector.unregisterFailureDetectionEventListener(s);
               this.gossiper.unregister(s);
            }

         });
         this.failureDetector.registerFailureDetectionEventListener(session);
         this.gossiper.register(session);
      }

      return session;
   }

   public synchronized CoordinatorSession getSession(UUID sessionId) {
      return (CoordinatorSession)this.sessions.get(sessionId);
   }

   public void handlePrepareResponse(InetAddress from, PrepareConsistentResponse msg) {
      CoordinatorSession session = this.getSession(msg.sessionID);
      if(session != null) {
         session.handlePrepareResponse(msg.participant, msg.success);
      }

   }
}
