package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.Versioned;

public class ValidationRequest extends RepairMessage<ValidationRequest> {
   public static Versioned<RepairVerbs.RepairVersion, RepairMessage.MessageSerializer<ValidationRequest>> serializers = RepairVerbs.RepairVersion.versioned((v) -> {
      return new RepairMessage.MessageSerializer<ValidationRequest>(v) {
         public void serialize(ValidationRequest message, DataOutputPlus out) throws IOException {
            ((Serializer)RepairJobDesc.serializers.get(this.version)).serialize(message.desc, out);
            out.writeInt(message.nowInSec);
         }

         public ValidationRequest deserialize(DataInputPlus dis) throws IOException {
            RepairJobDesc desc = (RepairJobDesc)((Serializer)RepairJobDesc.serializers.get(this.version)).deserialize(dis);
            return new ValidationRequest(desc, dis.readInt());
         }

         public long serializedSize(ValidationRequest message) {
            return ((Serializer)RepairJobDesc.serializers.get(this.version)).serializedSize(message.desc) + (long)TypeSizes.sizeof(message.nowInSec);
         }
      };
   });
   public final int nowInSec;

   public ValidationRequest(RepairJobDesc desc, int nowInSec) {
      super(desc);
      this.nowInSec = nowInSec;
   }

   public String toString() {
      return "ValidationRequest{nowInSec=" + this.nowInSec + "} " + super.toString();
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         ValidationRequest that = (ValidationRequest)o;
         return this.nowInSec == that.nowInSec;
      } else {
         return false;
      }
   }

   public int hashCode() {
      return this.nowInSec;
   }

   public RepairMessage.MessageSerializer<ValidationRequest> serializer(RepairVerbs.RepairVersion version) {
      return (RepairMessage.MessageSerializer)serializers.get(version);
   }

   public Optional<Verb<ValidationRequest, ?>> verb() {
      return Optional.of(Verbs.REPAIR.VALIDATION_REQUEST);
   }
}
