package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.Versioned;

public class ValidationComplete extends RepairMessage<ValidationComplete> {
   public static Versioned<RepairVerbs.RepairVersion, RepairMessage.MessageSerializer<ValidationComplete>> serializers = RepairVerbs.RepairVersion.versioned((v) -> {
      return new RepairMessage.MessageSerializer<ValidationComplete>(v) {
         public void serialize(ValidationComplete message, DataOutputPlus out) throws IOException {
            ((Serializer)RepairJobDesc.serializers.get(this.version)).serialize(message.desc, out);
            out.writeBoolean(message.success());
            if(message.trees != null) {
               ((MerkleTrees.MerkleTreesSerializer)MerkleTrees.serializers.get(this.version)).serialize(message.trees, out);
            }

         }

         public ValidationComplete deserialize(DataInputPlus in) throws IOException {
            RepairJobDesc desc = (RepairJobDesc)((Serializer)RepairJobDesc.serializers.get(this.version)).deserialize(in);
            boolean success = in.readBoolean();
            if(success) {
               MerkleTrees trees = ((MerkleTrees.MerkleTreesSerializer)MerkleTrees.serializers.get(this.version)).deserialize(in);
               return new ValidationComplete(desc, trees);
            } else {
               return new ValidationComplete(desc);
            }
         }

         public long serializedSize(ValidationComplete message) {
            long size = ((Serializer)RepairJobDesc.serializers.get(this.version)).serializedSize(message.desc);
            size += (long)TypeSizes.sizeof(message.success());
            if(message.trees != null) {
               size += ((MerkleTrees.MerkleTreesSerializer)MerkleTrees.serializers.get(this.version)).serializedSize(message.trees);
            }

            return size;
         }
      };
   });
   public final MerkleTrees trees;

   public ValidationComplete(RepairJobDesc desc) {
      super(desc);
      this.trees = null;
   }

   public RepairMessage.MessageSerializer<ValidationComplete> serializer(RepairVerbs.RepairVersion version) {
      return (RepairMessage.MessageSerializer)serializers.get(version);
   }

   public Optional<Verb<ValidationComplete, ?>> verb() {
      return Optional.of(Verbs.REPAIR.VALIDATION_COMPLETE);
   }

   public ValidationComplete(RepairJobDesc desc, MerkleTrees trees) {
      super(desc);

      assert trees != null;

      this.trees = trees;
   }

   public boolean success() {
      return this.trees != null;
   }

   public boolean equals(Object o) {
      if(!(o instanceof ValidationComplete)) {
         return false;
      } else {
         ValidationComplete other = (ValidationComplete)o;
         return this.desc.equals(other.desc);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.desc});
   }
}
