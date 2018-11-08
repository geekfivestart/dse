package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.Versioned;

public class SnapshotMessage extends RepairMessage<SnapshotMessage> {
   public static final Versioned<RepairVerbs.RepairVersion, RepairMessage.MessageSerializer<SnapshotMessage>> serializers = RepairVerbs.RepairVersion.versioned((v) -> {
      return new RepairMessage.MessageSerializer<SnapshotMessage>(v) {
         public void serialize(SnapshotMessage message, DataOutputPlus out) throws IOException {
            ((Serializer)RepairJobDesc.serializers.get(v)).serialize(message.desc, out);
         }

         public SnapshotMessage deserialize(DataInputPlus in) throws IOException {
            RepairJobDesc desc = (RepairJobDesc)((Serializer)RepairJobDesc.serializers.get(v)).deserialize(in);
            return new SnapshotMessage(desc);
         }

         public long serializedSize(SnapshotMessage message) {
            return ((Serializer)RepairJobDesc.serializers.get(v)).serializedSize(message.desc);
         }
      };
   });

   public SnapshotMessage(RepairJobDesc desc) {
      super(desc);
   }

   public RepairMessage.MessageSerializer<SnapshotMessage> serializer(RepairVerbs.RepairVersion version) {
      return (RepairMessage.MessageSerializer)serializers.get(version);
   }

   public Optional<Verb<SnapshotMessage, ?>> verb() {
      return Optional.of(Verbs.REPAIR.SNAPSHOT);
   }

   public String toString() {
      return this.desc.toString();
   }

   public boolean equals(Object o) {
      if(!(o instanceof SnapshotMessage)) {
         return false;
      } else {
         SnapshotMessage other = (SnapshotMessage)o;
         return this.desc.equals(other.desc);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.desc});
   }
}
