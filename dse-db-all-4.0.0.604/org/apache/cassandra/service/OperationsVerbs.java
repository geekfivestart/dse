package org.apache.cassandra.service;

import com.google.common.base.Throwables;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SnapshotCommand;
import org.apache.cassandra.db.Truncation;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.net.DroppedMessages;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.VerbGroup;
import org.apache.cassandra.net.VerbHandlers;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationsVerbs extends VerbGroup<OperationsVerbs.OperationsVersion> {
   private static final Logger logger = LoggerFactory.getLogger(OperationsVerbs.class);
   public Verb.RequestResponse<Truncation, TruncateResponse> TRUNCATE;
   public Verb.AckedRequest<SnapshotCommand> SNAPSHOT;
   public Verb.AckedRequest<EmptyPayload> REPLICATION_FINISHED;

   public OperationsVerbs(Verbs.Group id) {
      super(id, true, OperationsVerbs.OperationsVersion.class);
      VerbGroup<OperationsVerbs.OperationsVersion>.RegistrationHelper helper = this.helper();
      this.TRUNCATE = ((VerbGroup.RegistrationHelper.RequestResponseBuilder)((VerbGroup.RegistrationHelper.RequestResponseBuilder)((VerbGroup.RegistrationHelper.RequestResponseBuilder)helper.requestResponse("TRUNCATE", Truncation.class, TruncateResponse.class).requestStage(Stage.MISC)).droppedGroup(DroppedMessages.Group.TRUNCATE)).timeout(DatabaseDescriptor::getTruncateRpcTimeout)).syncHandler((from, t) -> {
         Tracing.trace("Applying truncation of {}.{}", t.keyspace, t.columnFamily);

         try {
            ColumnFamilyStore cfs = Keyspace.open(t.keyspace).getColumnFamilyStore(t.columnFamily);
            cfs.truncateBlocking();
            return new TruncateResponse(t.keyspace, t.columnFamily);
         } catch (Exception var4) {
            logger.error("Error in truncation", var4);
            FSError fsError = FSError.findNested(var4);
            throw Throwables.propagate((Throwable)(fsError == null?var4:fsError));
         }
      });
      this.SNAPSHOT = ((VerbGroup.RegistrationHelper.AckedRequestBuilder)((VerbGroup.RegistrationHelper.AckedRequestBuilder)((VerbGroup.RegistrationHelper.AckedRequestBuilder)helper.ackedRequest("SNAPSHOT", SnapshotCommand.class).requestStage(Stage.MISC)).droppedGroup(DroppedMessages.Group.SNAPSHOT)).timeout(DatabaseDescriptor::getRpcTimeout)).syncHandler((from, command) -> {
         if(command.clearSnapshot) {
            Keyspace.clearSnapshot(command.snapshotName, command.keyspace);
         } else {
            Keyspace.open(command.keyspace).getColumnFamilyStore(command.table).snapshot(command.snapshotName);
         }

      });
      this.REPLICATION_FINISHED = ((VerbGroup.RegistrationHelper.AckedRequestBuilder)((VerbGroup.RegistrationHelper.AckedRequestBuilder)((VerbGroup.RegistrationHelper.AckedRequestBuilder)helper.ackedRequest("REPLICATION_FINISHED", EmptyPayload.class).requestStage(Stage.MISC)).droppedGroup(DroppedMessages.Group.OTHER)).timeout(DatabaseDescriptor::getRpcTimeout)).syncHandler((from, x) -> {
         StorageService.instance.confirmReplication(from);
      });
   }

   public static enum OperationsVersion implements Version<OperationsVerbs.OperationsVersion> {
      OSS_30,
      DSE_60;

      private OperationsVersion() {
      }

      public static <T> Versioned<OperationsVerbs.OperationsVersion, T> versioned(Function<OperationsVerbs.OperationsVersion, ? extends T> function) {
         return new Versioned(OperationsVerbs.OperationsVersion.class, function);
      }
   }
}
