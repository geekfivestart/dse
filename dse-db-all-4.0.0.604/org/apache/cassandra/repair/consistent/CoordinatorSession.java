package org.apache.cassandra.repair.consistent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OneWayRequest;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.repair.RepairSessionResult;
import org.apache.cassandra.repair.messages.FailSession;
import org.apache.cassandra.repair.messages.FinalizeCommit;
import org.apache.cassandra.repair.messages.PrepareConsistentRequest;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoordinatorSession extends ConsistentSession implements IEndpointStateChangeSubscriber, IFailureDetectionEventListener {
   private static final Logger logger = LoggerFactory.getLogger(CoordinatorSession.class);
   private final Map<InetAddress, ConsistentSession.State> participantStates = new HashMap();
   private final SettableFuture<Boolean> prepareFuture = SettableFuture.create();
   private volatile long sessionStart = -9223372036854775808L;
   private volatile long repairStart = -9223372036854775808L;
   private volatile long finalizeStart = -9223372036854775808L;
   private volatile Consumer<CoordinatorSession> onCompleteCallback;

   public CoordinatorSession(CoordinatorSession.Builder builder) {
      super(builder);
      UnmodifiableIterator var2 = this.participants.iterator();

      while(var2.hasNext()) {
         InetAddress participant = (InetAddress)var2.next();
         this.participantStates.put(participant, ConsistentSession.State.PREPARING);
      }

   }

   public static CoordinatorSession.Builder builder() {
      return new CoordinatorSession.Builder();
   }

   public void setOnCompleteCallback(Consumer<CoordinatorSession> onCompleteCallback) {
      this.onCompleteCallback = onCompleteCallback;
   }

   public void setState(ConsistentSession.State state) {
      logger.trace("Setting coordinator state to {} for repair {}", state, this.sessionID);
      super.setState(state);
   }

   public synchronized void setParticipantState(InetAddress participant, ConsistentSession.State state) {
      if(!state.equals(this.participantStates.get(participant))) {
         logger.trace("Setting participant {} to state {} for repair {}", new Object[]{participant, state, this.sessionID});
         Preconditions.checkArgument(this.participantStates.containsKey(participant), "Session %s doesn't include %s", new Object[]{this.sessionID, participant});
         Preconditions.checkArgument(((ConsistentSession.State)this.participantStates.get(participant)).canTransitionTo(state), "Invalid state transition %s -> %s", new Object[]{this.participantStates.get(participant), state});
         this.participantStates.put(participant, state);
      }

      if(Iterables.all(this.participantStates.values(), (s) -> {
         return s == state;
      })) {
         this.setState(state);
      }

   }

   synchronized void setAll(ConsistentSession.State state) {
      UnmodifiableIterator var2 = this.participants.iterator();

      while(var2.hasNext()) {
         InetAddress participant = (InetAddress)var2.next();
         this.setParticipantState(participant, state);
      }

   }

   synchronized boolean allStates(ConsistentSession.State state) {
      return this.getState() == state && Iterables.all(this.participantStates.values(), (v) -> {
         return v == state;
      });
   }

   synchronized boolean anyState(ConsistentSession.State state) {
      return this.getState() == state || Iterables.any(this.participantStates.values(), (v) -> {
         return v == state;
      });
   }

   synchronized boolean hasFailed() {
      return this.getState() == ConsistentSession.State.FAILED || Iterables.any(this.participantStates.values(), (v) -> {
         return v == ConsistentSession.State.FAILED;
      });
   }

   @VisibleForTesting
   protected void send(OneWayRequest<? extends RepairMessage<?>> request) {
      logger.trace("Sending {} to {}", request.payload(), request.to());
      MessagingService.instance().send(request);
   }

   @VisibleForTesting
   protected void send(Request<? extends RepairMessage<?>, EmptyPayload> request, MessageCallback<EmptyPayload> callback) {
      logger.trace("Sending {} to {}", request.payload(), request.to());
      MessagingService.instance().send(request, callback);
   }

   public ListenableFuture<Boolean> prepare() {
      Preconditions.checkArgument(this.allStates(ConsistentSession.State.PREPARING));
      logger.debug("Beginning prepare phase of incremental repair session {}", this.sessionID);
      PrepareConsistentRequest message = new PrepareConsistentRequest(this.sessionID, this.coordinator, this.participants);
      UnmodifiableIterator var2 = this.participants.iterator();

      while(var2.hasNext()) {
         InetAddress participant = (InetAddress)var2.next();
         this.send(Verbs.REPAIR.CONSISTENT_REQUEST.newRequest(participant, message));
      }

      return this.prepareFuture;
   }

   public synchronized void handlePrepareResponse(InetAddress participant, boolean success) {
      if(this.getState() == ConsistentSession.State.FAILED) {
         logger.trace("Incremental repair session {} has failed, ignoring prepare response from {}", this.sessionID, participant);
      } else if(!success) {
         logger.debug("{} failed the prepare phase for incremental repair session {}. Aborting session", participant, this.sessionID);
         this.prepareFuture.set(Boolean.valueOf(false));
      } else {
         logger.trace("Successful prepare response received from {} for repair session {}", participant, this.sessionID);
         this.setParticipantState(participant, ConsistentSession.State.PREPARED);
         if(this.getState() == ConsistentSession.State.PREPARED) {
            logger.debug("Incremental repair session {} successfully prepared.", this.sessionID);
            this.prepareFuture.set(Boolean.valueOf(true));
         }
      }

   }

   public synchronized void setRepairing() {
      Preconditions.checkArgument(this.allStates(ConsistentSession.State.PREPARED));
      this.setAll(ConsistentSession.State.REPAIRING);
   }

   public synchronized ListenableFuture<Boolean> finalizeCommit() {
      Preconditions.checkArgument(this.allStates(ConsistentSession.State.REPAIRING));
      logger.debug("Committing finalization of repair session {}", this.sessionID);
      FinalizeCommit message = new FinalizeCommit(this.sessionID);
      SettableFuture<Boolean> finalizeResult = SettableFuture.create();
      CoordinatorSession.FinalizeCommitCallback callback = new CoordinatorSession.FinalizeCommitCallback(this.participants.size(), (response) -> {
         this.setParticipantState(response.from(), ConsistentSession.State.FINALIZED);
      }, finalizeResult);
      UnmodifiableIterator var4 = this.participants.iterator();

      while(var4.hasNext()) {
         InetAddress participant = (InetAddress)var4.next();
         this.send(Verbs.REPAIR.FINALIZE_COMMIT.newRequest(participant, message), callback);
      }

      return finalizeResult;
   }

   @VisibleForTesting
   protected synchronized void fail() {
      logger.info("Incremental repair session {} failed", this.sessionID);
      FailSession message = new FailSession(this.sessionID);
      if(!this.anyState(ConsistentSession.State.FINALIZED)) {
         UnmodifiableIterator var2 = this.participants.iterator();

         while(var2.hasNext()) {
            InetAddress participant = (InetAddress)var2.next();
            if(this.participantStates.get(participant) != ConsistentSession.State.FAILED && this.participantStates.get(participant) != ConsistentSession.State.FINALIZED) {
               this.send(Verbs.REPAIR.FAILED_SESSION.newRequest(participant, message));
               this.setParticipantState(participant, ConsistentSession.State.FAILED);
            }
         }
      } else {
         logger.info("Incremental repair session {} was finalized on some participants, do not send fail messages in order to allow for later resolution.", this.sessionID);
      }

      this.setState(ConsistentSession.State.FAILED);
      if(this.onCompleteCallback != null) {
         this.onCompleteCallback.accept(this);
      }

   }

   @VisibleForTesting
   protected void success() {
      logger.info("Incremental repair session {} completed", this.sessionID);
      this.setAll(ConsistentSession.State.FINALIZED);
      if(this.onCompleteCallback != null) {
         this.onCompleteCallback.accept(this);
      }

   }

   private static String formatDuration(long then, long now) {
      return then != -9223372036854775808L && now != -9223372036854775808L?DurationFormatUtils.formatDurationWords(now - then, true, true):"n/a";
   }

   public ListenableFuture execute(final Supplier<ListenableFuture<List<RepairSessionResult>>> sessionSubmitter, final AtomicBoolean hasFailure) {
      logger.info("Beginning coordination of incremental repair session {}", this.sessionID);
      this.sessionStart = System.currentTimeMillis();
      ListenableFuture<Boolean> prepareResult = this.prepare();
      ListenableFuture<List<RepairSessionResult>> repairResults = Futures.transform(prepareResult, new AsyncFunction<Boolean, List<RepairSessionResult>>() {
         public ListenableFuture<List<RepairSessionResult>> apply(Boolean success) throws Exception {
            CoordinatorSession.this.repairStart = System.currentTimeMillis();
            if(success.booleanValue()) {
               if(CoordinatorSession.logger.isDebugEnabled()) {
                  CoordinatorSession.logger.debug("Incremental repair {} prepare phase completed in {}", CoordinatorSession.this.sessionID, CoordinatorSession.formatDuration(CoordinatorSession.this.sessionStart, CoordinatorSession.this.repairStart));
               }

               CoordinatorSession.this.setRepairing();
               return (ListenableFuture)sessionSubmitter.get();
            } else {
               if(CoordinatorSession.logger.isDebugEnabled()) {
                  CoordinatorSession.logger.debug("Incremental repair {} prepare phase failed in {}", CoordinatorSession.this.sessionID, CoordinatorSession.formatDuration(CoordinatorSession.this.sessionStart, CoordinatorSession.this.repairStart));
               }

               return Futures.immediateFuture(null);
            }
         }
      });
      ListenableFuture<Boolean> finalizeResult = Futures.transform(repairResults, new AsyncFunction<List<RepairSessionResult>, Boolean>() {
         public ListenableFuture<Boolean> apply(List<RepairSessionResult> result) throws Exception {
            CoordinatorSession.this.finalizeStart = System.currentTimeMillis();
            if(result != null && !result.isEmpty() && !Iterables.any(result, (r) -> {
               return r == null;
            })) {
               if(CoordinatorSession.logger.isDebugEnabled()) {
                  CoordinatorSession.logger.debug("Incremental repair {} validation/stream phase completed in {}", CoordinatorSession.this.sessionID, CoordinatorSession.formatDuration(CoordinatorSession.this.repairStart, CoordinatorSession.this.finalizeStart));
               }

               return CoordinatorSession.this.finalizeCommit();
            } else {
               if(CoordinatorSession.logger.isDebugEnabled()) {
                  CoordinatorSession.logger.debug("Incremental repair {} validation/stream phase failed in {}", CoordinatorSession.this.sessionID, CoordinatorSession.formatDuration(CoordinatorSession.this.repairStart, CoordinatorSession.this.finalizeStart));
               }

               return Futures.immediateFuture(Boolean.valueOf(false));
            }
         }
      });
      Futures.addCallback(finalizeResult, new FutureCallback<Boolean>() {
         public void onSuccess(Boolean success) {
            if(!success.booleanValue()) {
               this.onFailure(new RuntimeException("Incremental repair failed!"));
            } else {
               CoordinatorSession.this.success();
               if(CoordinatorSession.logger.isDebugEnabled()) {
                  CoordinatorSession.logger.debug("Incremental repair {} completed in {}", CoordinatorSession.this.sessionID, CoordinatorSession.formatDuration(CoordinatorSession.this.sessionStart, System.currentTimeMillis()));
               }
            }

         }

         public void onFailure(Throwable t) {
            if(CoordinatorSession.logger.isDebugEnabled()) {
               CoordinatorSession.logger.debug("Incremental repair {} failed in {}", CoordinatorSession.this.sessionID, CoordinatorSession.formatDuration(CoordinatorSession.this.repairStart, System.currentTimeMillis()));
            }

            hasFailure.set(true);
            CoordinatorSession.this.fail();
         }
      });
      return finalizeResult;
   }

   public void onJoin(InetAddress endpoint, EndpointState epState) {
   }

   public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {
   }

   public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {
   }

   public void onAlive(InetAddress endpoint, EndpointState state) {
   }

   public void onDead(InetAddress endpoint, EndpointState state) {
   }

   public void onRemove(InetAddress endpoint) {
      this.convict(endpoint, 1.7976931348623157E308D);
   }

   public void onRestart(InetAddress endpoint, EndpointState epState) {
      this.convict(endpoint, 1.7976931348623157E308D);
   }

   public synchronized void convict(InetAddress endpoint, double phi) {
      if(this.participantStates.keySet().contains(endpoint)) {
         if(phi >= 2.0D * DatabaseDescriptor.getPhiConvictThreshold()) {
            String error = String.format("[repair #%s] Endpoint %s died, will fail incremental repair session.", new Object[]{this.sessionID, endpoint});
            if(this.getState() == ConsistentSession.State.PREPARING) {
               logger.warn(error);
               this.prepareFuture.set(Boolean.valueOf(false));
            }

         }
      }
   }

   private class FinalizeCommitCallback implements MessageCallback<EmptyPayload> {
      private int expectedResponses;
      private boolean hasError;
      private final Consumer<Response> onSuccessResponse;
      private final SettableFuture<Boolean> successFuture;

      public FinalizeCommitCallback(int expectedResponses, Consumer<Response> onSuccessResponse, SettableFuture<Boolean> successFuture) {
         this.expectedResponses = expectedResponses;
         this.onSuccessResponse = onSuccessResponse;
         this.successFuture = successFuture;
      }

      public synchronized void onTimeout(InetAddress host) {
         this.hasError = true;
         this.maybeUnblock();
      }

      public synchronized void onFailure(FailureResponse response) {
         this.hasError = true;
         this.maybeUnblock();
      }

      public synchronized void onResponse(Response response) {
         try {
            this.onSuccessResponse.accept(response);
         } finally {
            this.maybeUnblock();
         }

      }

      private void maybeUnblock() {
         if(--this.expectedResponses == 0) {
            this.successFuture.set(Boolean.valueOf(!this.hasError));
         }

      }
   }

   public static class Builder extends ConsistentSession.AbstractBuilder<CoordinatorSession.Builder> {
      public Builder() {
      }

      public CoordinatorSession build() {
         this.validate();
         return new CoordinatorSession(this);
      }
   }
}
