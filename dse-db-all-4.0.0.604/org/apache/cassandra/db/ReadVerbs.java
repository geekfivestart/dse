package org.apache.cassandra.db;

import io.reactivex.functions.Consumer;
import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.monitoring.Monitor;
import org.apache.cassandra.dht.BoundsVersion;
import org.apache.cassandra.net.DroppedMessages;
import org.apache.cassandra.net.ErrorHandler;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.VerbGroup;
import org.apache.cassandra.net.VerbHandlers;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;

public class ReadVerbs extends VerbGroup<ReadVerbs.ReadVersion> {
   private static final InetAddress local = FBUtilities.getBroadcastAddress();
   public final Verb.RequestResponse<SinglePartitionReadCommand, ReadResponse> SINGLE_READ;
   public final Verb.RequestResponse<PartitionRangeReadCommand, ReadResponse> RANGE_READ;
   final Verb.RequestResponse<NodeSyncReadCommand, ReadResponse> NODESYNC;

   private static <T extends ReadCommand> VerbHandlers.MonitoredRequestResponse<T, ReadResponse> readHandler() {
      return (from, command, monitor) -> {
         boolean isLocal = from.equals(local);
         if(StorageService.instance.isBootstrapMode() && !isLocal) {
            throw new RuntimeException("Cannot service reads while bootstrapping!");
         } else {
            if(Monitor.isTesting() && !SchemaConstants.isUserKeyspace(command.metadata().keyspace)) {
               monitor = null;
            }

            CompletableFuture<ReadResponse> result = new CompletableFuture();
            command.createResponse(command.executeLocally(monitor), isLocal).subscribe(result::complete, result::completeExceptionally);
            return result;
         }
      };
   }

   public ReadVerbs(Verbs.Group id) {
      super(id, false, ReadVerbs.ReadVersion.class);
      VerbGroup<ReadVerbs.ReadVersion>.RegistrationHelper helper = this.helper();
      this.SINGLE_READ = ((VerbGroup.RegistrationHelper.MonitoredRequestResponseBuilder)((VerbGroup.RegistrationHelper.MonitoredRequestResponseBuilder)helper.monitoredRequestResponse("SINGLE_READ", SinglePartitionReadCommand.class, ReadResponse.class).timeout(DatabaseDescriptor::getReadRpcTimeout)).droppedGroup(DroppedMessages.Group.READ)).handler(readHandler());
      this.RANGE_READ = ((VerbGroup.RegistrationHelper.MonitoredRequestResponseBuilder)((VerbGroup.RegistrationHelper.MonitoredRequestResponseBuilder)helper.monitoredRequestResponse("RANGE_READ", PartitionRangeReadCommand.class, ReadResponse.class).timeout(DatabaseDescriptor::getRangeRpcTimeout)).droppedGroup(DroppedMessages.Group.RANGE_SLICE)).handler(readHandler());
      this.NODESYNC = ((VerbGroup.RegistrationHelper.MonitoredRequestResponseBuilder)((VerbGroup.RegistrationHelper.MonitoredRequestResponseBuilder)((VerbGroup.RegistrationHelper.MonitoredRequestResponseBuilder)helper.monitoredRequestResponse("NODESYNC", NodeSyncReadCommand.class, ReadResponse.class).timeout(DatabaseDescriptor::getRangeRpcTimeout)).droppedGroup(DroppedMessages.Group.NODESYNC)).withErrorHandler((err) -> {
         switch(null.$SwitchMap$org$apache$cassandra$exceptions$RequestFailureReason[err.reason.ordinal()]) {
         case 1:
         case 2:
            ErrorHandler.noSpamLogger.debug(err.getMessage(), new Object[0]);
            break;
         default:
            ErrorHandler.DEFAULT.handleError(err);
         }

      })).handler(readHandler());
   }

   public static enum ReadVersion implements Version<ReadVerbs.ReadVersion> {
      OSS_30(EncodingVersion.OSS_30, BoundsVersion.OSS_30, DigestVersion.OSS_30),
      OSS_3014(EncodingVersion.OSS_30, BoundsVersion.OSS_30, DigestVersion.OSS_30),
      OSS_40(EncodingVersion.OSS_30, BoundsVersion.OSS_30, DigestVersion.OSS_30),
      DSE_60(EncodingVersion.OSS_30, BoundsVersion.OSS_30, DigestVersion.OSS_30);

      public final EncodingVersion encodingVersion;
      public final BoundsVersion boundsVersion;
      public final DigestVersion digestVersion;

      private ReadVersion(EncodingVersion encodingVersion, BoundsVersion boundsVersion, DigestVersion digestVersion) {
         this.encodingVersion = encodingVersion;
         this.boundsVersion = boundsVersion;
         this.digestVersion = digestVersion;
      }

      public static <T> Versioned<ReadVerbs.ReadVersion, T> versioned(Function<ReadVerbs.ReadVersion, ? extends T> function) {
         return new Versioned(ReadVerbs.ReadVersion.class, function);
      }
   }
}
