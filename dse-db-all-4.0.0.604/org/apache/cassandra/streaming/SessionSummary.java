package org.apache.cassandra.streaming;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.repair.messages.RepairVerbs;
import org.apache.cassandra.serializers.InetAddressSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public class SessionSummary implements Serializable {
   public static final Versioned<RepairVerbs.RepairVersion, Serializer<SessionSummary>> serializers = RepairVerbs.RepairVersion.versioned(SessionSummary.SessionSummarySerializer::new);
   public final InetAddress coordinator;
   public final InetAddress peer;
   public final Collection<StreamSummary> receivingSummaries;
   public final Collection<StreamSummary> sendingSummaries;

   public SessionSummary(InetAddress coordinator, InetAddress peer, Collection<StreamSummary> receivingSummaries, Collection<StreamSummary> sendingSummaries) {
      assert coordinator != null;

      assert peer != null;

      assert receivingSummaries != null;

      assert sendingSummaries != null;

      this.coordinator = coordinator;
      this.peer = peer;
      this.receivingSummaries = receivingSummaries;
      this.sendingSummaries = sendingSummaries;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         SessionSummary summary = (SessionSummary)o;
         return !this.coordinator.equals(summary.coordinator)?false:(!this.peer.equals(summary.peer)?false:(!this.receivingSummaries.equals(summary.receivingSummaries)?false:this.sendingSummaries.equals(summary.sendingSummaries)));
      } else {
         return false;
      }
   }

   public int hashCode() {
      int result = this.coordinator.hashCode();
      result = 31 * result + this.peer.hashCode();
      result = 31 * result + this.receivingSummaries.hashCode();
      result = 31 * result + this.sendingSummaries.hashCode();
      return result;
   }

   public static class SessionSummarySerializer extends VersionDependent<RepairVerbs.RepairVersion> implements Serializer<SessionSummary> {
      protected SessionSummarySerializer(RepairVerbs.RepairVersion version) {
         super(version);
      }

      public void serialize(SessionSummary summary, DataOutputPlus out) throws IOException {
         ByteBufferUtil.writeWithLength(InetAddressSerializer.instance.serialize(summary.coordinator), out);
         ByteBufferUtil.writeWithLength(InetAddressSerializer.instance.serialize(summary.peer), out);
         out.writeInt(summary.receivingSummaries.size());
         Iterator var3 = summary.receivingSummaries.iterator();

         StreamSummary streamSummary;
         while(var3.hasNext()) {
            streamSummary = (StreamSummary)var3.next();
            ((Serializer)StreamSummary.serializers.get(((RepairVerbs.RepairVersion)this.version).streamVersion)).serialize(streamSummary, out);
         }

         out.writeInt(summary.sendingSummaries.size());
         var3 = summary.sendingSummaries.iterator();

         while(var3.hasNext()) {
            streamSummary = (StreamSummary)var3.next();
            ((Serializer)StreamSummary.serializers.get(((RepairVerbs.RepairVersion)this.version).streamVersion)).serialize(streamSummary, out);
         }

      }

      public SessionSummary deserialize(DataInputPlus in) throws IOException {
         InetAddress coordinator = InetAddressSerializer.instance.deserialize(ByteBufferUtil.readWithLength(in));
         InetAddress peer = InetAddressSerializer.instance.deserialize(ByteBufferUtil.readWithLength(in));
         int numRcvd = in.readInt();
         ArrayList<StreamSummary> receivingSummaries = new ArrayList<StreamSummary>(numRcvd);
         for (int i = 0; i < numRcvd; ++i) {
            receivingSummaries.add(StreamSummary.serializers.get(((RepairVerbs.RepairVersion)this.version).streamVersion).deserialize(in));
         }
         int numSent = in.readInt();
         ArrayList<StreamSummary> sendingSummaries = new ArrayList<StreamSummary>(numRcvd);
         for (int i = 0; i < numSent; ++i) {
            sendingSummaries.add(StreamSummary.serializers.get(((RepairVerbs.RepairVersion)this.version).streamVersion).deserialize(in));
         }
         return new SessionSummary(coordinator, peer, receivingSummaries, sendingSummaries);
      }

      public long serializedSize(SessionSummary summary) {
         long size = 0L;
         size += (long)ByteBufferUtil.serializedSizeWithLength(InetAddressSerializer.instance.serialize(summary.coordinator));
         size += (long)ByteBufferUtil.serializedSizeWithLength(InetAddressSerializer.instance.serialize(summary.peer));
         size += (long)TypeSizes.sizeof(summary.receivingSummaries.size());

         Iterator var4;
         StreamSummary streamSummary;
         for(var4 = summary.receivingSummaries.iterator(); var4.hasNext(); size += ((Serializer)StreamSummary.serializers.get(((RepairVerbs.RepairVersion)this.version).streamVersion)).serializedSize(streamSummary)) {
            streamSummary = (StreamSummary)var4.next();
         }

         size += (long)TypeSizes.sizeof(summary.sendingSummaries.size());

         for(var4 = summary.sendingSummaries.iterator(); var4.hasNext(); size += ((Serializer)StreamSummary.serializers.get(((RepairVerbs.RepairVersion)this.version).streamVersion)).serializedSize(streamSummary)) {
            streamSummary = (StreamSummary)var4.next();
         }

         return size;
      }
   }
}
