package com.datastax.bdp.db.nodesync;

import com.google.common.annotations.VisibleForTesting;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.BoundsVersion;
import org.apache.cassandra.net.DroppedMessages;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.VerbGroup;
import org.apache.cassandra.net.VerbHandlers;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;

public class NodeSyncVerbs extends VerbGroup<NodeSyncVerbs.NodeSyncVersion> {
   final Verb.AckedRequest<UserValidationOptions> SUBMIT_VALIDATION;
   final Verb.AckedRequest<UserValidationID> CANCEL_VALIDATION;
   final Verb.AckedRequest<TracingOptions> ENABLE_TRACING;
   final Verb.AckedRequest<EmptyPayload> DISABLE_TRACING;
   final Verb.RequestResponse<EmptyPayload, Optional<UUID>> TRACING_SESSION;
   @VisibleForTesting
   static final Serializer<Optional<UUID>> tracingSessionResultSerializer;

   public NodeSyncVerbs(Verbs.Group id) {
      super(id, true, NodeSyncVersion.class);
      VerbGroup.RegistrationHelper helper = this.helper().stage(Stage.MISC).droppedGroup(DroppedMessages.Group.OTHER);
      NodeSyncService service = StorageService.instance.nodeSyncService;
      this.SUBMIT_VALIDATION = ((VerbGroup.RegistrationHelper.AckedRequestBuilder)((Object)helper.ackedRequest("SUBMIT_VALIDATION", UserValidationOptions.class).timeout(DatabaseDescriptor::getRpcTimeout))).syncHandler((from, options) -> service.startUserValidation((UserValidationOptions)options));
      this.CANCEL_VALIDATION = ((VerbGroup.RegistrationHelper.AckedRequestBuilder)((Object)helper.ackedRequest("CANCEL_VALIDATION", UserValidationID.class).timeout(DatabaseDescriptor::getRpcTimeout))).syncHandler((from, validationId) -> service.cancelUserValidation((UserValidationID)validationId));
      this.ENABLE_TRACING = ((VerbGroup.RegistrationHelper.AckedRequestBuilder)((Object)helper.ackedRequest("ENABLE_TRACING", TracingOptions.class).timeout(DatabaseDescriptor::getRpcTimeout))).syncHandler((from, options) -> service.enableTracing((TracingOptions)options));
      this.DISABLE_TRACING = ((VerbGroup.RegistrationHelper.AckedRequestBuilder)((Object)helper.ackedRequest("DISABLE_TRACING", EmptyPayload.class).timeout(DatabaseDescriptor::getRpcTimeout))).syncHandler((from, options) -> service.disableTracing());
      Class<?> optIdClass = Optional.empty().getClass();
      this.TRACING_SESSION = ((VerbGroup.RegistrationHelper.RequestResponseBuilder)((Object)((VerbGroup.RegistrationHelper.RequestResponseBuilder)((Object)helper.requestResponse("TRACING_SESSION", EmptyPayload.class, optIdClass).withResponseSerializer(tracingSessionResultSerializer))).timeout(DatabaseDescriptor::getRpcTimeout))).syncHandler((from, p) -> service.currentTracingSessionIfEnabled());
   }

   static {
      tracingSessionResultSerializer = Serializer.forOptional(UUIDSerializer.serializer);
   }

   public static enum NodeSyncVersion implements Version<NodeSyncVerbs.NodeSyncVersion> {
      DSE_603(BoundsVersion.OSS_30);

      public final BoundsVersion boundsVersion;

      private NodeSyncVersion(BoundsVersion boundsVersion) {
         this.boundsVersion = boundsVersion;
      }

      public static <T> Versioned<NodeSyncVerbs.NodeSyncVersion, T> versioned(Function<NodeSyncVerbs.NodeSyncVersion, ? extends T> function) {
         return new Versioned(NodeSyncVerbs.NodeSyncVersion.class, function);
      }
   }
}
