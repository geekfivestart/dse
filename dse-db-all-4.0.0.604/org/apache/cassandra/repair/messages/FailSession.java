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

public class FailSession extends ConsistentRepairMessage<FailSession> {
   public static final Versioned<RepairVerbs.RepairVersion, RepairMessage.MessageSerializer<FailSession>> serializers = RepairVerbs.RepairVersion.versioned((v) -> {
      return new RepairMessage.MessageSerializer<FailSession>(v) {
         public void serialize(FailSession msg, DataOutputPlus out) throws IOException {
            UUIDSerializer.serializer.serialize(msg.sessionID, out);
         }

         public FailSession deserialize(DataInputPlus in) throws IOException {
            return new FailSession(UUIDSerializer.serializer.deserialize(in));
         }

         public long serializedSize(FailSession msg) {
            return UUIDSerializer.serializer.serializedSize(msg.sessionID);
         }
      };
   });

   public FailSession(UUID sessionID) {
      super(sessionID);
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         FailSession that = (FailSession)o;
         return this.sessionID.equals(that.sessionID);
      } else {
         return false;
      }
   }

   public int hashCode() {
      return this.sessionID.hashCode();
   }

   public RepairMessage.MessageSerializer<FailSession> serializer(RepairVerbs.RepairVersion version) {
      return (RepairMessage.MessageSerializer)serializers.get(version);
   }

   public Optional<Verb<FailSession, ?>> verb() {
      return Optional.of(Verbs.REPAIR.FAILED_SESSION);
   }

   public boolean validate() {
      return true;
   }
}
