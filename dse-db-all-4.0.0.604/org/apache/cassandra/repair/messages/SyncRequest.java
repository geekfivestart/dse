package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.Versioned;

public class SyncRequest extends RepairMessage<SyncRequest> {
   public static Versioned<RepairVerbs.RepairVersion, RepairMessage.MessageSerializer<SyncRequest>> serializers = RepairVerbs.RepairVersion.versioned((v) -> {
      return new RepairMessage.MessageSerializer<SyncRequest>(v) {
         public void serialize(SyncRequest message, DataOutputPlus out) throws IOException {
            ((Serializer)RepairJobDesc.serializers.get(this.version)).serialize(message.desc, out);
            CompactEndpointSerializationHelper.serialize(message.initiator, out);
            CompactEndpointSerializationHelper.serialize(message.src, out);
            CompactEndpointSerializationHelper.serialize(message.dst, out);
            out.writeInt(message.ranges.size());
            Iterator var3 = message.ranges.iterator();

            while(var3.hasNext()) {
               Range range = (Range)var3.next();
               MessagingService.validatePartitioner((AbstractBounds)range);
               AbstractBounds.tokenSerializer.serialize(range, out, ((RepairVerbs.RepairVersion)this.version).boundsVersion);
            }

            out.writeInt(message.previewKind.getSerializationVal());
         }

         public SyncRequest deserialize(DataInputPlus in) throws IOException {
            RepairJobDesc desc = (RepairJobDesc)((Serializer)RepairJobDesc.serializers.get(this.version)).deserialize(in);
            InetAddress owner = CompactEndpointSerializationHelper.deserialize(in);
            InetAddress src = CompactEndpointSerializationHelper.deserialize(in);
            InetAddress dst = CompactEndpointSerializationHelper.deserialize(in);
            int rangesCount = in.readInt();
            List ranges = new ArrayList(rangesCount);

            for(int i = 0; i < rangesCount; ++i) {
               ranges.add((Range)AbstractBounds.tokenSerializer.deserialize(in, MessagingService.globalPartitioner(), ((RepairVerbs.RepairVersion)this.version).boundsVersion));
            }

            PreviewKind previewKind = PreviewKind.deserialize(in.readInt());
            return new SyncRequest(desc, owner, src, dst, ranges, previewKind);
         }

         public long serializedSize(SyncRequest message) {
            long size = ((Serializer)RepairJobDesc.serializers.get(this.version)).serializedSize(message.desc);
            size += (long)(3 * CompactEndpointSerializationHelper.serializedSize(message.initiator));
            size += (long)TypeSizes.sizeof(message.ranges.size());

            Range range;
            for(Iterator var4 = message.ranges.iterator(); var4.hasNext(); size += (long)AbstractBounds.tokenSerializer.serializedSize(range, ((RepairVerbs.RepairVersion)this.version).boundsVersion)) {
               range = (Range)var4.next();
            }

            size += (long)TypeSizes.sizeof(message.previewKind.getSerializationVal());
            return size;
         }
      };
   });
   public final InetAddress initiator;
   public final InetAddress src;
   public final InetAddress dst;
   public final Collection<Range<Token>> ranges;
   public final PreviewKind previewKind;

   public SyncRequest(RepairJobDesc desc, InetAddress initiator, InetAddress src, InetAddress dst, Collection<Range<Token>> ranges, PreviewKind previewKind) {
      super(desc);
      this.initiator = initiator;
      this.src = src;
      this.dst = dst;
      this.ranges = ranges;
      this.previewKind = previewKind;
   }

   public boolean equals(Object o) {
      if(!(o instanceof SyncRequest)) {
         return false;
      } else {
         SyncRequest req = (SyncRequest)o;
         return this.desc.equals(req.desc) && this.initiator.equals(req.initiator) && this.src.equals(req.src) && this.dst.equals(req.dst) && this.ranges.equals(req.ranges) && this.previewKind == req.previewKind;
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.desc, this.initiator, this.src, this.dst, this.ranges, this.previewKind});
   }

   public RepairMessage.MessageSerializer<SyncRequest> serializer(RepairVerbs.RepairVersion version) {
      return (RepairMessage.MessageSerializer)serializers.get(version);
   }

   public Optional<Verb<SyncRequest, ?>> verb() {
      return Optional.of(Verbs.REPAIR.SYNC_REQUEST);
   }

   public String toString() {
      return "SyncRequest{initiator=" + this.initiator + ", src=" + this.src + ", dst=" + this.dst + ", ranges=" + this.ranges + ", previewKind=" + this.previewKind + "} " + super.toString();
   }
}
