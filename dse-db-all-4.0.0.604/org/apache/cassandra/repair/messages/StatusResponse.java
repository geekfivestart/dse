package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.consistent.ConsistentSession;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.Versioned;

public class StatusResponse extends ConsistentRepairMessage<StatusResponse> {
   public static final Versioned<RepairVerbs.RepairVersion, RepairMessage.MessageSerializer<StatusResponse>> serializers = RepairVerbs.RepairVersion.versioned((v) -> {
      return new RepairMessage.MessageSerializer<StatusResponse>(v) {
         public void serialize(StatusResponse msg, DataOutputPlus out) throws IOException {
            UUIDSerializer.serializer.serialize(msg.sessionID, out);
            out.writeInt(msg.state.ordinal());
            out.writeBoolean(msg.running);
         }

         public StatusResponse deserialize(DataInputPlus in) throws IOException {
            return new StatusResponse(UUIDSerializer.serializer.deserialize(in), ConsistentSession.State.valueOf(in.readInt()), in.readBoolean());
         }

         public long serializedSize(StatusResponse msg) {
            return UUIDSerializer.serializer.serializedSize(msg.sessionID) + (long)TypeSizes.sizeof(msg.state.ordinal()) + (long)TypeSizes.sizeof(msg.running);
         }
      };
   });
   public final ConsistentSession.State state;
   public final boolean running;

   public StatusResponse(UUID sessionID, ConsistentSession.State state) {
      this(sessionID, state, false);
   }

   public StatusResponse(UUID sessionID, ConsistentSession.State state, boolean running) {
      super(sessionID);

      assert state != null;

      this.state = state;
      this.running = running;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         StatusResponse that = (StatusResponse)o;
         return this.sessionID.equals(that.sessionID) && this.state == that.state && this.running == that.running;
      } else {
         return false;
      }
   }

   public int hashCode() {
      int result = this.sessionID.hashCode();
      result = 31 * result + this.state.hashCode();
      return this.running?result * 31:result;
   }

   public String toString() {
      return "StatusResponse{sessionID=" + this.sessionID + ", state=" + this.state + ", running=" + this.running + '}';
   }

   public RepairMessage.MessageSerializer<StatusResponse> serializer(RepairVerbs.RepairVersion version) {
      return (RepairMessage.MessageSerializer)serializers.get(version);
   }

   public Optional<Verb<StatusResponse, ?>> verb() {
      return Optional.empty();
   }
}
