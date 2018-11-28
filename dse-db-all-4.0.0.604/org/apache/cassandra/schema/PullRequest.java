package org.apache.cassandra.schema;

import java.io.IOException;
import java.util.function.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.Versioned;

public class PullRequest {
   public static Versioned<SchemaVerbs.SchemaVersion, Serializer<PullRequest>> serializers = SchemaVerbs.SchemaVersion.versioned((v) -> {
      return new Serializer<PullRequest>() {
         public void serialize(PullRequest pullRequest, DataOutputPlus out) throws IOException {
            if(v.compareTo(SchemaVerbs.SchemaVersion.DSE_603) >= 0) {
               out.writeInt(pullRequest.schemaCompatibilityVersion);
            }

         }

         public PullRequest deserialize(DataInputPlus in) throws IOException {
            int schemaCompatibilityVersion = v.compareTo(SchemaVerbs.SchemaVersion.DSE_603) >= 0?in.readInt():-1;
            return new PullRequest(schemaCompatibilityVersion);
         }

         public long serializedSize(PullRequest pullRequest) {
            return v.compareTo(SchemaVerbs.SchemaVersion.DSE_603) >= 0?(long)TypeSizes.sizeof(pullRequest.schemaCompatibilityVersion):0L;
         }
      };
   });
   private final int schemaCompatibilityVersion;

   private PullRequest(int schemaCompatibilityVersion) {
      this.schemaCompatibilityVersion = schemaCompatibilityVersion;
   }

   static PullRequest create() {
      return new PullRequest(0);
   }

   int schemaCompatibilityVersion() {
      return this.schemaCompatibilityVersion;
   }
}
