package org.apache.cassandra.repair.messages;

import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.BoundsVersion;
import org.apache.cassandra.net.DroppedMessages;
import org.apache.cassandra.net.MessagingVersion;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.VerbGroup;
import org.apache.cassandra.net.VerbHandlers;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.consistent.CoordinatorSessions;
import org.apache.cassandra.repair.consistent.LocalSessions;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;

public class RepairVerbs extends VerbGroup<RepairVerbs.RepairVersion> {
   private static final String MIXED_MODE_ERROR = "Some nodes involved in repair are on an incompatible major version. Repair is not supported in mixed major version clusters.";
   static final int FINALIZE_COMMIT_TIMEOUT;
   public final Verb.OneWay<ValidationRequest> VALIDATION_REQUEST;
   public final Verb.OneWay<ValidationComplete> VALIDATION_COMPLETE;
   public final Verb.OneWay<SyncRequest> SYNC_REQUEST;
   public final Verb.OneWay<SyncComplete> SYNC_COMPLETE;
   public final Verb.AckedRequest<PrepareMessage> PREPARE;
   public final Verb.AckedRequest<SnapshotMessage> SNAPSHOT;
   public final Verb.AckedRequest<CleanupMessage> CLEANUP;
   public final Verb.OneWay<PrepareConsistentRequest> CONSISTENT_REQUEST;
   public final Verb.OneWay<PrepareConsistentResponse> CONSISTENT_RESPONSE;
   public final Verb.AckedRequest<FinalizeCommit> FINALIZE_COMMIT;
   public final Verb.OneWay<FailSession> FAILED_SESSION;
   public final Verb.RequestResponse<StatusRequest, StatusResponse> STATUS_REQUEST;

   public RepairVerbs(final Verbs.Group id) {
      super(id, true, RepairVersion.class);
      final RegistrationHelper helper = this.helper().stage(Stage.ANTI_ENTROPY).droppedGroup(DroppedMessages.Group.REPAIR);
      this.VALIDATION_REQUEST = helper.oneWay("VALIDATION_REQUEST", ValidationRequest.class).handler(decoratedOneWay(ActiveRepairService.instance::handleValidationRequest));
      this.VALIDATION_COMPLETE = helper.oneWay("VALIDATION_COMPLETE", ValidationComplete.class).handler(decoratedOneWay(ActiveRepairService.instance::handleValidationComplete));
      this.SYNC_REQUEST = helper.oneWay("SYNC_REQUEST", SyncRequest.class).handler(decoratedOneWay(ActiveRepairService.instance::handleSyncRequest));
      this.SYNC_COMPLETE = helper.oneWay("SYNC_COMPLETE", SyncComplete.class).handler(decoratedOneWay(ActiveRepairService.instance::handleSyncComplete));
      this.PREPARE = helper.ackedRequest("PREPARE", PrepareMessage.class).timeout(DatabaseDescriptor::getRpcTimeout).syncHandler(decoratedAck(ActiveRepairService.instance::handlePrepare));
      this.SNAPSHOT = helper.ackedRequest("SNAPSHOT", SnapshotMessage.class).timeout(1, TimeUnit.HOURS).syncHandler(decoratedAck(ActiveRepairService.instance::handleSnapshot));
      this.CLEANUP = helper.ackedRequest("CLEANUP", CleanupMessage.class).timeout(1, TimeUnit.HOURS).syncHandler((from, msg) -> ActiveRepairService.instance.removeParentRepairSession(msg.parentRepairSession));
      this.CONSISTENT_REQUEST = helper.oneWay("CONSISTENT_REQUEST", PrepareConsistentRequest.class).handler(decoratedOneWay(ActiveRepairService.instance.consistent.local::handlePrepareMessage));
      this.CONSISTENT_RESPONSE = helper.oneWay("CONSISTENT_RESPONSE", PrepareConsistentResponse.class).handler(ActiveRepairService.instance.consistent.coordinated::handlePrepareResponse);
      this.FINALIZE_COMMIT = helper.ackedRequest("FINALIZE_COMMIT", FinalizeCommit.class).timeout(RepairVerbs.FINALIZE_COMMIT_TIMEOUT, TimeUnit.SECONDS).syncHandler(decoratedAck((from, msg) -> {
         ActiveRepairService.instance.consistent.local.handleFinalizeCommitMessage(from, msg);
         maybeRemoveSession(from, msg);
         return;
      }));
      this.FAILED_SESSION = helper.oneWay("FAILED_SESSION", FailSession.class).handler(decoratedOneWay((from, msg) -> {
         ActiveRepairService.instance.consistent.local.handleFailSessionMessage(from, msg);
         maybeRemoveSession(from, msg);
         return;
      }));
      this.STATUS_REQUEST = helper.requestResponse("STATUS_REQUEST", StatusRequest.class, StatusResponse.class).timeout(DatabaseDescriptor::getRpcTimeout).syncHandler(ActiveRepairService.instance.consistent.local::handleStatusRequest);
   }

   public String getUnsupportedVersionMessage(MessagingVersion version) {
      return "Some nodes involved in repair are on an incompatible major version. Repair is not supported in mixed major version clusters.";
   }

   private static <P extends RepairMessage> VerbHandlers.OneWay<P> decoratedOneWay(VerbHandlers.OneWay<P> handler) {
      return (from, message) -> {
         try {
            if(message.validate()) {
               handler.handle(from, message);
            }
         } catch (Exception var4) {
            removeSessionAndRethrow(message, var4);
         }

      };
   }

   private static <P extends RepairMessage> VerbHandlers.SyncAckedRequest<P> decoratedAck(VerbHandlers.SyncAckedRequest<P> handler) {
      return (from, message) -> {
         try {
            if(message.validate()) {
               handler.handle2(from, message);
            }
         } catch (Exception var4) {
            removeSessionAndRethrow(message, var4);
         }

      };
   }

   private static void maybeRemoveSession(InetAddress from, ConsistentRepairMessage msg) {
      if(!from.equals(FBUtilities.getBroadcastAddress())) {
         ActiveRepairService.instance.removeParentRepairSession(msg.sessionID);
      }

   }

   private static void removeSessionAndRethrow(RepairMessage msg, Exception e) {
      RepairJobDesc desc = msg.desc;
      if(desc != null && desc.parentSessionId != null) {
         ActiveRepairService.instance.removeParentRepairSession(desc.parentSessionId);
      }

      throw Throwables.propagate(e);
   }

   static {
      FINALIZE_COMMIT_TIMEOUT = Integer.getInteger("cassandra.finalize_commit_timeout_seconds", Ints.checkedCast(TimeUnit.MINUTES.toSeconds(10L))).intValue();
   }

   public static enum RepairVersion implements Version<RepairVerbs.RepairVersion> {
      OSS_40(BoundsVersion.OSS_30, StreamMessage.StreamVersion.OSS_40),
      DSE_60(BoundsVersion.OSS_30, StreamMessage.StreamVersion.DSE_60);

      public final BoundsVersion boundsVersion;
      public final StreamMessage.StreamVersion streamVersion;

      private RepairVersion(BoundsVersion boundsVersion, StreamMessage.StreamVersion streamVersion) {
         this.boundsVersion = boundsVersion;
         this.streamVersion = streamVersion;
      }

      public static <T> Versioned<RepairVerbs.RepairVersion, T> versioned(Function<RepairVerbs.RepairVersion, ? extends T> function) {
         return new Versioned(RepairVerbs.RepairVersion.class, function);
      }
   }
}
