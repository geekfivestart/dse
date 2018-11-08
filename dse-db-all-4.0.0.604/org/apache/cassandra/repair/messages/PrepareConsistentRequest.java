package org.apache.cassandra.repair.messages;

import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
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

public class PrepareConsistentRequest extends ConsistentRepairMessage<PrepareConsistentRequest> {
   public static Versioned<RepairVerbs.RepairVersion, RepairMessage.MessageSerializer<PrepareConsistentRequest>> serializers = RepairVerbs.RepairVersion.versioned((v) -> {
      return new RepairMessage.MessageSerializer<PrepareConsistentRequest>(v) {
         private final TypeSerializer inetSerializer;

         {
            this.inetSerializer = InetAddressSerializer.instance;
         }

         public void serialize(PrepareConsistentRequest request, DataOutputPlus out) throws IOException {
            UUIDSerializer.serializer.serialize(request.sessionID, out);
            ByteBufferUtil.writeWithShortLength(this.inetSerializer.serialize(request.coordinator), out);
            out.writeInt(request.participants.size());
            Iterator var3 = request.participants.iterator();

            while(var3.hasNext()) {
               InetAddress peer = (InetAddress)var3.next();
               ByteBufferUtil.writeWithShortLength(this.inetSerializer.serialize(peer), out);
            }

         }

         public PrepareConsistentRequest deserialize(DataInputPlus in) throws IOException {
            UUID sessionId = UUIDSerializer.serializer.deserialize(in);
            InetAddress coordinator = (InetAddress)this.inetSerializer.deserialize(ByteBufferUtil.readWithShortLength(in));
            int numPeers = in.readInt();
            Set peers = new HashSet(numPeers);

            for(int i = 0; i < numPeers; ++i) {
               InetAddress peer = (InetAddress)this.inetSerializer.deserialize(ByteBufferUtil.readWithShortLength(in));
               peers.add(peer);
            }

            return new PrepareConsistentRequest(sessionId, coordinator, peers);
         }

         public long serializedSize(PrepareConsistentRequest request) {
            long size = UUIDSerializer.serializer.serializedSize(request.sessionID);
            size += (long)ByteBufferUtil.serializedSizeWithShortLength(this.inetSerializer.serialize(request.coordinator));
            size += (long)TypeSizes.sizeof(request.participants.size());

            InetAddress peer;
            for(Iterator var4 = request.participants.iterator(); var4.hasNext(); size += (long)ByteBufferUtil.serializedSizeWithShortLength(this.inetSerializer.serialize(peer))) {
               peer = (InetAddress)var4.next();
            }

            return size;
         }
      };
   });
   public final InetAddress coordinator;
   public final Set<InetAddress> participants;

   public PrepareConsistentRequest(UUID sessionID, InetAddress coordinator, Set<InetAddress> participants) {
      super(sessionID);

      assert coordinator != null;

      assert participants != null && !participants.isEmpty();

      this.coordinator = coordinator;
      this.participants = ImmutableSet.copyOf(participants);
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         PrepareConsistentRequest that = (PrepareConsistentRequest)o;
         return this.sessionID.equals(that.sessionID) && this.coordinator.equals(that.coordinator) && this.participants.equals(that.participants);
      } else {
         return false;
      }
   }

   public int hashCode() {
      int result = this.sessionID.hashCode();
      result = 31 * result + this.coordinator.hashCode();
      result = 31 * result + this.participants.hashCode();
      return result;
   }

   public String toString() {
      return "PrepareConsistentRequest{sessionID=" + this.sessionID + ", coordinator=" + this.coordinator + ", participants=" + this.participants + '}';
   }

   public RepairMessage.MessageSerializer<PrepareConsistentRequest> serializer(RepairVerbs.RepairVersion version) {
      return (RepairMessage.MessageSerializer)serializers.get(version);
   }

   public Optional<Verb<PrepareConsistentRequest, ?>> verb() {
      return Optional.of(Verbs.REPAIR.CONSISTENT_REQUEST);
   }
}
