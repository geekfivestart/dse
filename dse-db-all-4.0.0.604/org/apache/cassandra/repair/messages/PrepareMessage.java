package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.Versioned;

public class PrepareMessage extends RepairMessage<PrepareMessage> {
   public static final Versioned<RepairVerbs.RepairVersion, RepairMessage.MessageSerializer<PrepareMessage>> serializers = RepairVerbs.RepairVersion.versioned((v) -> {
      return new RepairMessage.MessageSerializer<PrepareMessage>(v) {
         public void serialize(PrepareMessage message, DataOutputPlus out) throws IOException {
            out.writeInt(message.tableIds.size());
            Iterator var3 = message.tableIds.iterator();

            while(var3.hasNext()) {
               TableId tableId = (TableId)var3.next();
               tableId.serialize(out);
            }

            UUIDSerializer.serializer.serialize(message.parentRepairSession, out);
            out.writeInt(message.ranges.size());
            var3 = message.ranges.iterator();

            while(var3.hasNext()) {
               Range r = (Range)var3.next();
               MessagingService.validatePartitioner((AbstractBounds)r);
               Range.tokenSerializer.serialize(r, out, ((RepairVerbs.RepairVersion)this.version).boundsVersion);
            }

            out.writeBoolean(message.isIncremental);
            out.writeLong(message.timestamp);
            out.writeInt(message.previewKind.getSerializationVal());
         }

         public PrepareMessage deserialize(DataInputPlus in) throws IOException {
            int tableIdCount = in.readInt();
            List tableIds = new ArrayList(tableIdCount);

            for(int i = 0; i < tableIdCount; ++i) {
               tableIds.add(TableId.deserialize(in));
            }

            UUID parentRepairSession = UUIDSerializer.serializer.deserialize(in);
            int rangeCount = in.readInt();
            List ranges = new ArrayList(rangeCount);

            for(int ix = 0; ix < rangeCount; ++ix) {
               ranges.add((Range)Range.tokenSerializer.deserialize(in, MessagingService.globalPartitioner(), ((RepairVerbs.RepairVersion)this.version).boundsVersion));
            }

            boolean isIncremental = in.readBoolean();
            long timestamp = in.readLong();
            PreviewKind previewKind = PreviewKind.deserialize(in.readInt());
            return new PrepareMessage(parentRepairSession, tableIds, ranges, isIncremental, timestamp, previewKind);
         }

         public long serializedSize(PrepareMessage message) {
            long size = (long)TypeSizes.sizeof(message.tableIds.size());

            Iterator var4;
            TableId tableId;
            for(var4 = message.tableIds.iterator(); var4.hasNext(); size += (long)tableId.serializedSize()) {
               tableId = (TableId)var4.next();
            }

            size += UUIDSerializer.serializer.serializedSize(message.parentRepairSession);
            size += (long)TypeSizes.sizeof(message.ranges.size());

            Range r;
            for(var4 = message.ranges.iterator(); var4.hasNext(); size += (long)Range.tokenSerializer.serializedSize(r, ((RepairVerbs.RepairVersion)this.version).boundsVersion)) {
               r = (Range)var4.next();
            }

            size += (long)TypeSizes.sizeof(message.isIncremental);
            size += (long)TypeSizes.sizeof(message.timestamp);
            size += (long)TypeSizes.sizeof(message.previewKind.getSerializationVal());
            return size;
         }
      };
   });
   public final List<TableId> tableIds;
   public final Collection<Range<Token>> ranges;
   public final UUID parentRepairSession;
   public final boolean isIncremental;
   public final long timestamp;
   public final PreviewKind previewKind;

   public PrepareMessage(UUID parentRepairSession, List<TableId> tableIds, Collection<Range<Token>> ranges, boolean isIncremental, long timestamp, PreviewKind previewKind) {
      super((RepairJobDesc)null);
      this.parentRepairSession = parentRepairSession;
      this.tableIds = tableIds;
      this.ranges = ranges;
      this.isIncremental = isIncremental;
      this.timestamp = timestamp;
      this.previewKind = previewKind;
   }

   public boolean equals(Object o) {
      if(!(o instanceof PrepareMessage)) {
         return false;
      } else {
         PrepareMessage other = (PrepareMessage)o;
         return this.parentRepairSession.equals(other.parentRepairSession) && this.isIncremental == other.isIncremental && this.timestamp == other.timestamp && this.tableIds.equals(other.tableIds) && this.ranges.equals(other.ranges);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.parentRepairSession, Boolean.valueOf(this.isIncremental), Long.valueOf(this.timestamp), this.tableIds, this.ranges});
   }

   public RepairMessage.MessageSerializer<PrepareMessage> serializer(RepairVerbs.RepairVersion version) {
      return (RepairMessage.MessageSerializer)serializers.get(version);
   }

   public Optional<Verb<PrepareMessage, ?>> verb() {
      return Optional.of(Verbs.REPAIR.PREPARE);
   }

   public String toString() {
      return "PrepareMessage{tableIds='" + this.tableIds + '\'' + ", ranges=" + this.ranges + ", parentRepairSession=" + this.parentRepairSession + ", isIncremental=" + this.isIncremental + ", timestamp=" + this.timestamp + '}';
   }
}
