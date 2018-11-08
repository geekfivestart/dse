package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.repair.NodePair;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.streaming.SessionSummary;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.Versioned;

public class SyncComplete extends RepairMessage<SyncComplete> {
   public final NodePair nodes;
   public final boolean success;
   public final List<SessionSummary> summaries;
   public static final Versioned<RepairVerbs.RepairVersion, RepairMessage.MessageSerializer<SyncComplete>> serializers = RepairVerbs.RepairVersion.versioned((v) -> {
      return new RepairMessage.MessageSerializer<SyncComplete>(v) {
         public void serialize(SyncComplete message, DataOutputPlus out) throws IOException {
            ((Serializer)RepairJobDesc.serializers.get(this.version)).serialize(message.desc, out);
            NodePair.serializer.serialize(message.nodes, out);
            out.writeBoolean(message.success);
            out.writeInt(message.summaries.size());
            Iterator var3 = message.summaries.iterator();

            while(var3.hasNext()) {
               SessionSummary summary = (SessionSummary)var3.next();
               ((Serializer)SessionSummary.serializers.get(this.version)).serialize(summary, out);
            }

         }

         public SyncComplete deserialize(DataInputPlus in) throws IOException {
            RepairJobDesc desc = (RepairJobDesc)((Serializer)RepairJobDesc.serializers.get(this.version)).deserialize(in);
            NodePair nodes = (NodePair)NodePair.serializer.deserialize(in);
            boolean success = in.readBoolean();
            int numSummaries = in.readInt();
            List summaries = new ArrayList(numSummaries);

            for(int i = 0; i < numSummaries; ++i) {
               summaries.add(((Serializer)SessionSummary.serializers.get(this.version)).deserialize(in));
            }

            return new SyncComplete(desc, nodes, success, summaries);
         }

         public long serializedSize(SyncComplete message) {
            long size = ((Serializer)RepairJobDesc.serializers.get(this.version)).serializedSize(message.desc);
            size += NodePair.serializer.serializedSize(message.nodes);
            size += (long)TypeSizes.sizeof(message.success);
            size += (long)TypeSizes.sizeof(message.summaries.size());

            SessionSummary summary;
            for(Iterator var4 = message.summaries.iterator(); var4.hasNext(); size += ((Serializer)SessionSummary.serializers.get(this.version)).serializedSize(summary)) {
               summary = (SessionSummary)var4.next();
            }

            return size;
         }
      };
   });

   public SyncComplete(RepairJobDesc desc, NodePair nodes, boolean success, List<SessionSummary> summaries) {
      super(desc);
      this.nodes = nodes;
      this.success = success;
      this.summaries = summaries;
   }

   public SyncComplete(RepairJobDesc desc, InetAddress endpoint1, InetAddress endpoint2, boolean success, List<SessionSummary> summaries) {
      super(desc);
      this.nodes = new NodePair(endpoint1, endpoint2);
      this.success = success;
      this.summaries = summaries;
   }

   public boolean equals(Object o) {
      if(!(o instanceof SyncComplete)) {
         return false;
      } else {
         SyncComplete other = (SyncComplete)o;
         return this.desc.equals(other.desc) && this.success == other.success && this.nodes.equals(other.nodes) && this.summaries.equals(other.summaries);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.desc, Boolean.valueOf(this.success), this.nodes, this.summaries});
   }

   public RepairMessage.MessageSerializer<SyncComplete> serializer(RepairVerbs.RepairVersion version) {
      return (RepairMessage.MessageSerializer)serializers.get(version);
   }

   public Optional<Verb<SyncComplete, ?>> verb() {
      return Optional.of(Verbs.REPAIR.SYNC_COMPLETE);
   }
}
