package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.Versioned;

public class CleanupMessage extends RepairMessage<CleanupMessage> {
   public static Versioned<RepairVerbs.RepairVersion, RepairMessage.MessageSerializer<CleanupMessage>> serializers = RepairVerbs.RepairVersion.versioned((v) -> {
      return new RepairMessage.MessageSerializer<CleanupMessage>(v) {
         public void serialize(CleanupMessage message, DataOutputPlus out) throws IOException {
            UUIDSerializer.serializer.serialize(message.parentRepairSession, out);
         }

         public CleanupMessage deserialize(DataInputPlus in) throws IOException {
            UUID parentRepairSession = UUIDSerializer.serializer.deserialize(in);
            return new CleanupMessage(parentRepairSession);
         }

         public long serializedSize(CleanupMessage message) {
            return UUIDSerializer.serializer.serializedSize(message.parentRepairSession);
         }
      };
   });
   public final UUID parentRepairSession;

   public CleanupMessage(UUID parentRepairSession) {
      super((RepairJobDesc)null);
      this.parentRepairSession = parentRepairSession;
   }

   public boolean equals(Object o) {
      if(!(o instanceof CleanupMessage)) {
         return false;
      } else {
         CleanupMessage other = (CleanupMessage)o;
         return this.parentRepairSession.equals(other.parentRepairSession);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.parentRepairSession});
   }

   public RepairMessage.MessageSerializer<CleanupMessage> serializer(RepairVerbs.RepairVersion version) {
      return (RepairMessage.MessageSerializer)serializers.get(version);
   }

   public Optional<Verb<CleanupMessage, ?>> verb() {
      return Optional.of(Verbs.REPAIR.CLEANUP);
   }
}
