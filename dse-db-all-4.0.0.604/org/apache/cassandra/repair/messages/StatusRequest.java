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

public class StatusRequest extends ConsistentRepairMessage<StatusRequest> {
   public static Versioned<RepairVerbs.RepairVersion, RepairMessage.MessageSerializer<StatusRequest>> serializers = RepairVerbs.RepairVersion.versioned((v) -> {
      return new RepairMessage.MessageSerializer<StatusRequest>(v) {
         public void serialize(StatusRequest msg, DataOutputPlus out) throws IOException {
            UUIDSerializer.serializer.serialize(msg.sessionID, out);
         }

         public StatusRequest deserialize(DataInputPlus in) throws IOException {
            return new StatusRequest(UUIDSerializer.serializer.deserialize(in));
         }

         public long serializedSize(StatusRequest msg) {
            return UUIDSerializer.serializer.serializedSize(msg.sessionID);
         }
      };
   });

   public StatusRequest(UUID sessionID) {
      super(sessionID);
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         StatusRequest request = (StatusRequest)o;
         return this.sessionID.equals(request.sessionID);
      } else {
         return false;
      }
   }

   public int hashCode() {
      return this.sessionID.hashCode();
   }

   public String toString() {
      return "StatusRequest{sessionID=" + this.sessionID + '}';
   }

   public RepairMessage.MessageSerializer<StatusRequest> serializer(RepairVerbs.RepairVersion version) {
      return (RepairMessage.MessageSerializer)serializers.get(version);
   }

   public Optional<Verb<StatusRequest, ?>> verb() {
      return Optional.of(Verbs.REPAIR.STATUS_REQUEST);
   }
}
