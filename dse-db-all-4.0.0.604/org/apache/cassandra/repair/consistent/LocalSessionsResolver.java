package org.apache.cassandra.repair.consistent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.UnmodifiableIterator;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.repair.messages.StatusRequest;
import org.apache.cassandra.repair.messages.StatusResponse;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalSessionsResolver implements Callable<Boolean> {
   private static final Logger logger = LoggerFactory.getLogger(LocalSessionsResolver.class);
   private final LocalSession newSession;
   private final Collection<LocalSession> localSessions;
   private final Consumer<Pair<LocalSession, ConsistentSession.State>> resolver;

   public LocalSessionsResolver(LocalSession newSession, Collection<LocalSession> localSessions, Consumer<Pair<LocalSession, ConsistentSession.State>> resolver) {
      this.newSession = newSession;
      this.localSessions = localSessions;
      this.resolver = resolver;
   }

   public Boolean call() throws Exception {
      Collection<LocalSession> sessionsToResolve = (Collection)this.localSessions.stream().filter((s) -> {
         return !s.sessionID.equals(this.newSession.sessionID) && !s.isCompleted() && s.ranges.stream().anyMatch((candidate) -> {
            return this.newSession.ranges.stream().anyMatch((parent) -> {
               return parent.intersects(candidate);
            });
         });
      }).collect(Collectors.toList());
      logger.info("Found {} local sessions to resolve.", Integer.valueOf(sessionsToResolve.size()));
      Iterator var2 = sessionsToResolve.iterator();

      while(var2.hasNext()) {
         LocalSession session = (LocalSession)var2.next();
         synchronized(session) {
            if(!session.isCompleted()) {
               logger.info("Trying to resolve local session {}.", session.sessionID);
               Optional<StatusResponse> localResponse = Optional.empty();
               if(session.participants.contains(this.getBroadcastAddress())) {
                  localResponse = this.getStatusFromLocalNode(session);
                  logger.debug("Got status {} from local node {} for session {}.", new Object[]{localResponse, this.getBroadcastAddress(), session.sessionID});
               }

               LocalSessionsResolver.Resolution status = this.tryResolveFromLocalNode(localResponse);
               if(status == LocalSessionsResolver.Resolution.RUNNING) {
                  logger.info("Cannot resolve local session {} coordinated by {} because still running on ranges intersecting with new session {} coordinated by {}. Only one incremental repair session can be running for any given range, so the new session will not be started.", new Object[]{session.sessionID, session.coordinator, this.newSession.sessionID, this.newSession.coordinator});
                  return Boolean.valueOf(false);
               }

               if(status == LocalSessionsResolver.Resolution.PENDING) {
                  LocalSessionsResolver.StatusResponseCallback remoteResponses = new LocalSessionsResolver.StatusResponseCallback(session);
                  int responses = 0;
                  UnmodifiableIterator var9 = session.participants.iterator();

                  while(var9.hasNext()) {
                     InetAddress participant = (InetAddress)var9.next();
                     if(!participant.equals(this.getBroadcastAddress())) {
                        ++responses;
                        this.send(Verbs.REPAIR.STATUS_REQUEST.newRequest(participant, new StatusRequest(session.sessionID)), remoteResponses);
                     }
                  }

                  status = this.tryResolveFromParticipants(localResponse, remoteResponses.await(responses));
               }

               if(status != LocalSessionsResolver.Resolution.FINALIZED && status != LocalSessionsResolver.Resolution.FAILED) {
                  logger.info("Cannot resolve local session {} based on participant responses.", session.sessionID);
                  return Boolean.valueOf(false);
               }

               ConsistentSession.State state = LocalSessionsResolver.Resolution.from(status);
               logger.info("Resolving local session {} with state {}.", session.sessionID, state);
               long start = System.currentTimeMillis();
               this.resolver.accept(Pair.create(session, state));
               logger.info("Resolved local session {} with state {}, took: {}ms.", new Object[]{session.sessionID, state, Long.valueOf(System.currentTimeMillis() - start)});
            }
         }
      }

      return Boolean.valueOf(true);
   }

   @VisibleForTesting
   protected InetAddress getBroadcastAddress() {
      return FBUtilities.getBroadcastAddress();
   }

   @VisibleForTesting
   protected Optional<StatusResponse> getStatusFromLocalNode(LocalSession session) {
      return Optional.of(new StatusResponse(session.sessionID, session.getState(), this.isRunningLocally(session)));
   }

   @VisibleForTesting
   protected void send(Request<StatusRequest, StatusResponse> request, MessageCallback<StatusResponse> callback) {
      MessagingService.instance().send(request, callback);
   }

   private boolean isComplete(ConsistentSession.State state) {
      return state == ConsistentSession.State.FAILED || state == ConsistentSession.State.FINALIZED;
   }

   private boolean isRunningLocally(LocalSession session) {
      return ActiveRepairService.instance.hasParentRepairSession(session.sessionID);
   }

   private LocalSessionsResolver.Resolution tryResolveFromLocalNode(Optional<StatusResponse> localResponse) {
      if(localResponse.isPresent()) {
         if(!((StatusResponse)localResponse.get()).running && this.isComplete(((StatusResponse)localResponse.get()).state)) {
            return LocalSessionsResolver.Resolution.from(((StatusResponse)localResponse.get()).state);
         }

         if(((StatusResponse)localResponse.get()).running) {
            return LocalSessionsResolver.Resolution.RUNNING;
         }
      }

      return LocalSessionsResolver.Resolution.PENDING;
   }

   private LocalSessionsResolver.Resolution tryResolveFromParticipants(Optional<StatusResponse> localResponse, List<Optional<StatusResponse>> remoteResponses) {
      Optional<Optional<StatusResponse>> completedResponse = remoteResponses.stream().filter((p) -> {
         return p.isPresent() && this.isComplete(((StatusResponse)p.get()).state);
      }).findFirst();
      if(!completedResponse.isPresent()) {
         List<Optional<StatusResponse>> all = new ArrayList(remoteResponses.size() + 1);
         all.add(localResponse);
         all.addAll(remoteResponses);
         Optional<Integer> pendingCounter = all.stream().filter((p) -> {
            return p.isPresent() && !((StatusResponse)p.get()).running && !this.isComplete(((StatusResponse)p.get()).state);
         }).map((p) -> {
            return Integer.valueOf(1);
         }).reduce((c1, c2) -> {
            return Integer.valueOf(c1.intValue() + c2.intValue());
         });
         return pendingCounter.isPresent() && ((Integer)pendingCounter.get()).equals(Integer.valueOf(all.size()))?LocalSessionsResolver.Resolution.from(ConsistentSession.State.FAILED):LocalSessionsResolver.Resolution.PENDING;
      } else {
         return LocalSessionsResolver.Resolution.from(((StatusResponse)((Optional)completedResponse.get()).get()).state);
      }
   }

   private class StatusResponseCallback implements MessageCallback<StatusResponse> {
      private final Object monitor = new Object();
      private List<Optional<StatusResponse>> responses = new LinkedList();
      private final LocalSession session;

      public StatusResponseCallback(LocalSession session) {
         this.session = session;
      }

      public void onResponse(Response<StatusResponse> response) {
         Object var2 = this.monitor;
         synchronized(this.monitor) {
            StatusResponse status = (StatusResponse)response.payload();
            LocalSessionsResolver.logger.debug("Got status {} from remote node {} for session {}.", new Object[]{status, response.from(), status.sessionID});
            this.responses.add(Optional.of(status));
            this.monitor.notifyAll();
         }
      }

      public void onFailure(FailureResponse<StatusResponse> failure) {
         Object var2 = this.monitor;
         synchronized(this.monitor) {
            LocalSessionsResolver.logger.debug("Failed to get status from remote node {} for session {}, reason: {}.", new Object[]{failure.from(), this.session.sessionID, failure.reason()});
            this.responses.add(Optional.empty());
            this.monitor.notifyAll();
         }
      }

      public void onTimeout(InetAddress host) {
         Object var2 = this.monitor;
         synchronized(this.monitor) {
            LocalSessionsResolver.logger.debug("Failed to get status from remote node {} for session {} due to timeout.", host, this.session.sessionID);
            this.responses.add(Optional.empty());
            this.monitor.notifyAll();
         }
      }

      public List<Optional<StatusResponse>> await(int expectedResponses) {
         LocalSessionsResolver.logger.debug("Awaiting {} remote responses for session {}.", Integer.valueOf(expectedResponses), this.session.sessionID);
         Object var2 = this.monitor;
         synchronized(this.monitor) {
            while(expectedResponses > this.responses.size()) {
               try {
                  this.monitor.wait();
               } catch (InterruptedException var5) {
                  Thread.currentThread().interrupt();
               }
            }

            LocalSessionsResolver.logger.debug("Got {} remote responses for session {}.", Integer.valueOf(expectedResponses), this.session.sessionID);
            return this.responses;
         }
      }
   }

   private static enum Resolution {
      FINALIZED,
      FAILED,
      RUNNING,
      PENDING;

      private Resolution() {
      }

      public static ConsistentSession.State from(LocalSessionsResolver.Resolution from) {
         switch(null.$SwitchMap$org$apache$cassandra$repair$consistent$LocalSessionsResolver$Resolution[from.ordinal()]) {
         case 1:
            return ConsistentSession.State.FINALIZED;
         case 2:
            return ConsistentSession.State.FAILED;
         default:
            throw new IllegalStateException(String.format("Cannot convert from %s", new Object[]{from}));
         }
      }

      public static LocalSessionsResolver.Resolution from(ConsistentSession.State from) {
         switch(null.$SwitchMap$org$apache$cassandra$repair$consistent$ConsistentSession$State[from.ordinal()]) {
         case 1:
            return FINALIZED;
         case 2:
            return FAILED;
         default:
            throw new IllegalStateException(String.format("Cannot convert from %s", new Object[]{from}));
         }
      }
   }
}
