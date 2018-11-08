package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.serializers.InetAddressSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.Versioned;

public class PrepareConsistentResponse extends ConsistentRepairMessage<PrepareConsistentResponse> {
   public static Versioned<RepairVerbs.RepairVersion, RepairMessage.MessageSerializer<PrepareConsistentResponse>> serializers = RepairVerbs.RepairVersion.versioned((v) -> {
      return new RepairMessage.MessageSerializer<PrepareConsistentResponse>(v) {
         private final TypeSerializer inetSerializer;

         {
            this.inetSerializer = InetAddressSerializer.instance;
         }

         public void serialize(PrepareConsistentResponse response, DataOutputPlus out) throws IOException {
            UUIDSerializer.serializer.serialize(response.sessionID, out);
            ByteBufferUtil.writeWithShortLength(this.inetSerializer.serialize(response.participant), out);
            out.writeBoolean(response.success);
         }

         public PrepareConsistentResponse deserialize(DataInputPlus in) throws IOException {
            return new PrepareConsistentResponse(UUIDSerializer.serializer.deserialize(in), (InetAddress)this.inetSerializer.deserialize(ByteBufferUtil.readWithShortLength(in)), in.readBoolean());
         }

         public long serializedSize(PrepareConsistentResponse response) {
            long size = UUIDSerializer.serializer.serializedSize(response.sessionID);
            size += (long)ByteBufferUtil.serializedSizeWithShortLength(this.inetSerializer.serialize(response.participant));
            size += (long)TypeSizes.sizeof(response.success);
            return size;
         }
      };
   });
   public final InetAddress participant;
   public final boolean success;

   public PrepareConsistentResponse(UUID sessionID, InetAddress participant, boolean success) {
      super(sessionID);

      assert participant != null;

      this.participant = participant;
      this.success = success;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         PrepareConsistentResponse that = (PrepareConsistentResponse)o;
         return this.success == that.success && this.sessionID.equals(that.sessionID) && this.participant.equals(that.participant);
      } else {
         return false;
      }
   }

   public int hashCode() {
      int result = this.sessionID.hashCode();
      result = 31 * result + this.participant.hashCode();
      result = 31 * result + (this.success?1:0);
      return result;
   }

   public String toString() {
      return "PrepareConsistentResponse{sessionID=" + this.sessionID + ", participant=" + this.participant + ", success=" + this.success + '}';
   }

   public RepairMessage.MessageSerializer<PrepareConsistentResponse> serializer(RepairVerbs.RepairVersion version) {
      return (RepairMessage.MessageSerializer)serializers.get(version);
   }

   public Optional<Verb<PrepareConsistentResponse, ?>> verb() {
      return Optional.of(Verbs.REPAIR.CONSISTENT_RESPONSE);
   }
}
