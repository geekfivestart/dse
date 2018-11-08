package org.apache.cassandra.hints;

import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.net.DroppedMessages;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.VerbGroup;
import org.apache.cassandra.net.VerbHandlers;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HintsVerbs extends VerbGroup<HintsVerbs.HintsVersion> {
   private static final Logger logger = LoggerFactory.getLogger(HintsVerbs.class);
   public final Verb.AckedRequest<HintMessage> HINT;
   private static final VerbHandlers.AckedRequest<HintMessage> HINT_HANDLER = (from, msg) -> {
      UUID hostId = msg.hostId;
      InetAddress address = StorageService.instance.getEndpointForHostId(hostId);

      Hint hint;
      try {
         hint = msg.hint();
      } catch (UnknownTableException var7) {
         logger.trace("Failed to decode and apply a hint for {} ({}) - table with id {} is unknown", new Object[]{address, hostId, var7.id});
         return null;
      }

      try {
         hint.mutation.getPartitionUpdates().forEach(PartitionUpdate::validate);
      } catch (MarshalException var6) {
         logger.warn("Failed to validate a hint for {} ({}) - skipped", address, hostId);
         return null;
      }

      return !hostId.equals(StorageService.instance.getLocalHostUUID())?CompletableFuture.supplyAsync(() -> {
         HintsService.instance.write(hostId, hint);
         return null;
      }, (cmd) -> {
         StageManager.getStage(Stage.BACKGROUND_IO).execute(cmd, ExecutorLocals.create());
      }):(!StorageProxy.instance.appliesLocally(hint.mutation)?CompletableFuture.supplyAsync(() -> {
         HintsService.instance.writeForAllReplicas(hint);
         return null;
      }, (cmd) -> {
         StageManager.getStage(Stage.BACKGROUND_IO).execute(cmd, ExecutorLocals.create());
      }):hint.applyFuture());
   };

   public HintsVerbs(Verbs.Group id) {
      super(id, true, HintsVerbs.HintsVersion.class);
      VerbGroup<HintsVerbs.HintsVersion>.RegistrationHelper helper = this.helper().droppedGroup(DroppedMessages.Group.HINT);
      this.HINT = ((VerbGroup.RegistrationHelper.AckedRequestBuilder)((VerbGroup.RegistrationHelper.AckedRequestBuilder)helper.ackedRequest("HINT", HintMessage.class).timeout(DatabaseDescriptor::getWriteRpcTimeout)).withBackPressure()).handler(HINT_HANDLER);
   }

   public static enum HintsVersion implements Version<HintsVerbs.HintsVersion> {
      OSS_30(EncodingVersion.OSS_30);

      public final EncodingVersion encodingVersion;

      private HintsVersion(EncodingVersion encodingVersion) {
         this.encodingVersion = encodingVersion;
      }

      public int code() {
         return this.ordinal() + 1;
      }

      public static HintsVerbs.HintsVersion fromCode(int code) {
         return values()[code - 1];
      }

      public static <T> Versioned<HintsVerbs.HintsVersion, T> versioned(Function<HintsVerbs.HintsVersion, ? extends T> function) {
         return new Versioned(HintsVerbs.HintsVersion.class, function);
      }
   }
}
