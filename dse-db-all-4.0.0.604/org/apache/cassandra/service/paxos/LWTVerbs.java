package org.apache.cassandra.service.paxos;

import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.net.DroppedMessages;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.VerbGroup;
import org.apache.cassandra.net.VerbHandlers;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.utils.BooleanSerializer;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;

public class LWTVerbs extends VerbGroup<LWTVerbs.LWTVersion> {
   public final Verb.RequestResponse<Commit, PrepareResponse> PREPARE;
   public final Verb.RequestResponse<Commit, Boolean> PROPOSE;
   public final Verb.AckedRequest<Commit> COMMIT;

   public LWTVerbs(Verbs.Group id) {
      super(id, false, LWTVerbs.LWTVersion.class);
      VerbGroup<LWTVerbs.LWTVersion>.RegistrationHelper helper = this.helper().executeOnIOScheduler().droppedGroup(DroppedMessages.Group.LWT);
      this.PREPARE = ((VerbGroup.RegistrationHelper.RequestResponseBuilder)((VerbGroup.RegistrationHelper.RequestResponseBuilder)helper.requestResponse("PREPARE", Commit.class, PrepareResponse.class).timeout(DatabaseDescriptor::getWriteRpcTimeout)).requestExecutor(TPC.ioScheduler().forTaskType(TPCTaskType.LWT_PREPARE))).syncHandler((from, commit) -> {
         return PaxosState.prepare(commit);
      });
      this.PROPOSE = ((VerbGroup.RegistrationHelper.RequestResponseBuilder)((VerbGroup.RegistrationHelper.RequestResponseBuilder)((VerbGroup.RegistrationHelper.RequestResponseBuilder)helper.requestResponse("PROPOSE", Commit.class, Boolean.class).timeout(DatabaseDescriptor::getWriteRpcTimeout)).requestExecutor(TPC.ioScheduler().forTaskType(TPCTaskType.LWT_PROPOSE))).withResponseSerializer(BooleanSerializer.serializer)).syncHandler((from, commit) -> {
         return PaxosState.propose(commit);
      });
      this.COMMIT = ((VerbGroup.RegistrationHelper.AckedRequestBuilder)((VerbGroup.RegistrationHelper.AckedRequestBuilder)helper.ackedRequest("COMMIT", Commit.class).timeout(DatabaseDescriptor::getWriteRpcTimeout)).requestExecutor(TPC.ioScheduler().forTaskType(TPCTaskType.LWT_COMMIT))).syncHandler((from, commit) -> {
         PaxosState.commit(commit);
      });
   }

   public static enum LWTVersion implements Version<LWTVerbs.LWTVersion> {
      OSS_30(EncodingVersion.OSS_30);

      public final EncodingVersion encodingVersion;

      private LWTVersion(EncodingVersion encodingVersion) {
         this.encodingVersion = encodingVersion;
      }

      public static <T> Versioned<LWTVerbs.LWTVersion, T> versioned(Function<LWTVerbs.LWTVersion, ? extends T> function) {
         return new Versioned(LWTVerbs.LWTVersion.class, function);
      }
   }
}
