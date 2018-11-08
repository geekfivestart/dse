package org.apache.cassandra.repair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.RepairVerbs;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public class RepairJobDesc {
   public static final Versioned<RepairVerbs.RepairVersion, Serializer<RepairJobDesc>> serializers = RepairVerbs.RepairVersion.versioned((x$0) -> {
      return new RepairJobDesc.RepairJobDescSerializer(x$0);
   });
   public final UUID parentSessionId;
   public final UUID sessionId;
   public final String keyspace;
   public final String columnFamily;
   public final Collection<Range<Token>> ranges;

   public RepairJobDesc(UUID parentSessionId, UUID sessionId, String keyspace, String columnFamily, Collection<Range<Token>> ranges) {
      this.parentSessionId = parentSessionId;
      this.sessionId = sessionId;
      this.keyspace = keyspace;
      this.columnFamily = columnFamily;
      this.ranges = ranges;
   }

   public String toString() {
      return "[repair #" + this.sessionId + " on " + this.keyspace + "/" + this.columnFamily + ", " + this.ranges + "]";
   }

   public String toString(PreviewKind previewKind) {
      return '[' + previewKind.logPrefix() + " #" + this.sessionId + " on " + this.keyspace + "/" + this.columnFamily + ", " + this.ranges + "]";
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         RepairJobDesc that = (RepairJobDesc)o;
         if(!this.columnFamily.equals(that.columnFamily)) {
            return false;
         } else if(!this.keyspace.equals(that.keyspace)) {
            return false;
         } else {
            if(this.ranges != null) {
               if(that.ranges == null || this.ranges.size() != that.ranges.size() || this.ranges.size() == that.ranges.size() && !this.ranges.containsAll(that.ranges)) {
                  return false;
               }
            } else if(that.ranges != null) {
               return false;
            }

            if(!this.sessionId.equals(that.sessionId)) {
               return false;
            } else {
               if(this.parentSessionId != null) {
                  if(!this.parentSessionId.equals(that.parentSessionId)) {
                     return false;
                  }
               } else if(that.parentSessionId != null) {
                  return false;
               }

               return true;
            }
         }
      } else {
         return false;
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.sessionId, this.keyspace, this.columnFamily, this.ranges});
   }

   private static class RepairJobDescSerializer extends VersionDependent<RepairVerbs.RepairVersion> implements Serializer<RepairJobDesc> {
      private RepairJobDescSerializer(RepairVerbs.RepairVersion version) {
         super(version);
      }

      public void serialize(RepairJobDesc desc, DataOutputPlus out) throws IOException {
         out.writeBoolean(desc.parentSessionId != null);
         if(desc.parentSessionId != null) {
            UUIDSerializer.serializer.serialize(desc.parentSessionId, out);
         }

         UUIDSerializer.serializer.serialize(desc.sessionId, out);
         out.writeUTF(desc.keyspace);
         out.writeUTF(desc.columnFamily);
         MessagingService.validatePartitioner(desc.ranges);
         out.writeInt(desc.ranges.size());
         Iterator var3 = desc.ranges.iterator();

         while(var3.hasNext()) {
            Range<Token> rt = (Range)var3.next();
            AbstractBounds.tokenSerializer.serialize(rt, out, ((RepairVerbs.RepairVersion)this.version).boundsVersion);
         }

      }

      public RepairJobDesc deserialize(DataInputPlus in) throws IOException {
         UUID parentSessionId = null;
         if(in.readBoolean()) {
            parentSessionId = UUIDSerializer.serializer.deserialize(in);
         }

         UUID sessionId = UUIDSerializer.serializer.deserialize(in);
         String keyspace = in.readUTF();
         String columnFamily = in.readUTF();
         int nRanges = in.readInt();
         Collection<Range<Token>> ranges = new ArrayList(nRanges);

         for(int i = 0; i < nRanges; ++i) {
            Range<Token> range = (Range)AbstractBounds.tokenSerializer.deserialize(in, MessagingService.globalPartitioner(), ((RepairVerbs.RepairVersion)this.version).boundsVersion);
            ranges.add(range);
         }

         return new RepairJobDesc(parentSessionId, sessionId, keyspace, columnFamily, ranges);
      }

      public long serializedSize(RepairJobDesc desc) {
         long size = (long)TypeSizes.sizeof(desc.parentSessionId != null);
         if(desc.parentSessionId != null) {
            size += UUIDSerializer.serializer.serializedSize(desc.parentSessionId);
         }

         size += UUIDSerializer.serializer.serializedSize(desc.sessionId);
         size += (long)TypeSizes.sizeof(desc.keyspace);
         size += (long)TypeSizes.sizeof(desc.columnFamily);
         size += (long)TypeSizes.sizeof(desc.ranges.size());

         Range rt;
         for(Iterator var4 = desc.ranges.iterator(); var4.hasNext(); size += (long)AbstractBounds.tokenSerializer.serializedSize(rt, ((RepairVerbs.RepairVersion)this.version).boundsVersion)) {
            rt = (Range)var4.next();
         }

         return size;
      }
   }
}
