package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.Versioned;

public class FinalizeCommit extends ConsistentRepairMessage<FinalizeCommit> {
   public static Versioned<RepairVerbs.RepairVersion, RepairMessage.MessageSerializer<FinalizeCommit>> serializers = RepairVerbs.RepairVersion.versioned((v) -> {
      return new RepairMessage.MessageSerializer<FinalizeCommit>(v) {
         public void serialize(FinalizeCommit msg, DataOutputPlus out) throws IOException {
            UUIDSerializer.serializer.serialize(msg.sessionID, out);
         }

         public FinalizeCommit deserialize(DataInputPlus in) throws IOException {
            return new FinalizeCommit(UUIDSerializer.serializer.deserialize(in));
         }

         public long serializedSize(FinalizeCommit msg) {
            return UUIDSerializer.serializer.serializedSize(msg.sessionID);
         }
      };
   });

   public FinalizeCommit(UUID sessionID) {
      super(sessionID);
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         FinalizeCommit that = (FinalizeCommit)o;
         return this.sessionID.equals(that.sessionID);
      } else {
         return false;
      }
   }

   public int hashCode() {
      return this.sessionID.hashCode();
   }

   public String toString() {
      return "FinalizeCommit{sessionID=" + this.sessionID + '}';
   }

   public RepairMessage.MessageSerializer<FinalizeCommit> serializer(RepairVerbs.RepairVersion version) {
      return (RepairMessage.MessageSerializer)serializers.get(version);
   }

   public Optional<Verb<FinalizeCommit, ?>> verb() {
      return Optional.of(Verbs.REPAIR.FINALIZE_COMMIT);
   }
}
