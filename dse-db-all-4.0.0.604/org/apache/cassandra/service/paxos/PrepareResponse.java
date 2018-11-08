package org.apache.cassandra.service.paxos;

import java.io.IOException;
import java.util.function.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public class PrepareResponse {
   public static final Versioned<LWTVerbs.LWTVersion, Serializer<PrepareResponse>> serializers = LWTVerbs.LWTVersion.versioned((x$0) -> {
      return new PrepareResponse.PrepareSerializer(x$0);
   });
   public final boolean promised;
   public final Commit inProgressCommit;
   public final Commit mostRecentCommit;

   public PrepareResponse(boolean promised, Commit inProgressCommit, Commit mostRecentCommit) {
      assert inProgressCommit.update.partitionKey().equals(mostRecentCommit.update.partitionKey());

      assert inProgressCommit.update.metadata().id.equals(mostRecentCommit.update.metadata().id);

      this.promised = promised;
      this.mostRecentCommit = mostRecentCommit;
      this.inProgressCommit = inProgressCommit;
   }

   public String toString() {
      return String.format("PrepareResponse(%s, %s, %s)", new Object[]{Boolean.valueOf(this.promised), this.mostRecentCommit, this.inProgressCommit});
   }

   public static class PrepareSerializer extends VersionDependent<LWTVerbs.LWTVersion> implements Serializer<PrepareResponse> {
      private final Commit.CommitSerializer commitSerializer;

      private PrepareSerializer(LWTVerbs.LWTVersion version) {
         super(version);
         this.commitSerializer = (Commit.CommitSerializer)Commit.serializers.get(version);
      }

      public void serialize(PrepareResponse response, DataOutputPlus out) throws IOException {
         out.writeBoolean(response.promised);
         this.commitSerializer.serialize(response.inProgressCommit, out);
         this.commitSerializer.serialize(response.mostRecentCommit, out);
      }

      public PrepareResponse deserialize(DataInputPlus in) throws IOException {
         boolean success = in.readBoolean();
         Commit inProgress = this.commitSerializer.deserialize(in);
         Commit mostRecent = this.commitSerializer.deserialize(in);
         return new PrepareResponse(success, inProgress, mostRecent);
      }

      public long serializedSize(PrepareResponse response) {
         return (long)TypeSizes.sizeof(response.promised) + this.commitSerializer.serializedSize(response.inProgressCommit) + this.commitSerializer.serializedSize(response.mostRecentCommit);
      }
   }
}
