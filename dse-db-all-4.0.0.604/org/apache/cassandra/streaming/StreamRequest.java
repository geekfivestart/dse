package org.apache.cassandra.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.BoundsVersion;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.Versioned;

public class StreamRequest {
   public static final Versioned<StreamMessage.StreamVersion, Serializer<StreamRequest>> serializers = StreamMessage.StreamVersion.versioned((v) -> {
      return new Serializer<StreamRequest>() {
         public void serialize(StreamRequest request, DataOutputPlus out) throws IOException {
            out.writeUTF(request.keyspace);
            out.writeInt(request.ranges.size());
            Iterator var3 = request.ranges.iterator();

            while(var3.hasNext()) {
               Range range = (Range)var3.next();
               MessagingService.validatePartitioner((AbstractBounds)range);
               Token.serializer.serialize((Token)range.left, out, v.boundsVersion);
               Token.serializer.serialize((Token)range.right, out, v.boundsVersion);
            }

            out.writeInt(request.columnFamilies.size());
            var3 = request.columnFamilies.iterator();

            while(var3.hasNext()) {
               String cf = (String)var3.next();
               out.writeUTF(cf);
            }

         }

         public StreamRequest deserialize(DataInputPlus in) throws IOException {
            String keyspace = in.readUTF();
            int rangeCount = in.readInt();
            List ranges = new ArrayList(rangeCount);

            int cfCount;
            for(cfCount = 0; cfCount < rangeCount; ++cfCount) {
               Token left = Token.serializer.deserialize(in, MessagingService.globalPartitioner(), (BoundsVersion)v.boundsVersion);
               Token right = Token.serializer.deserialize(in, MessagingService.globalPartitioner(), (BoundsVersion)v.boundsVersion);
               ranges.add(new Range(left, right));
            }

            cfCount = in.readInt();
            List columnFamilies = new ArrayList(cfCount);

            for(int i = 0; i < cfCount; ++i) {
               columnFamilies.add(in.readUTF());
            }

            return new StreamRequest(keyspace, ranges, columnFamilies);
         }

         public long serializedSize(StreamRequest request) {
            long size = (long)TypeSizes.sizeof(request.keyspace);
            size += (long)TypeSizes.sizeof(request.ranges.size());

            Iterator var4;
            Range range;
            for(var4 = request.ranges.iterator(); var4.hasNext(); size += (long)Token.serializer.serializedSize((Token)range.right, v.boundsVersion)) {
               range = (Range)var4.next();
               size += (long)Token.serializer.serializedSize((Token)range.left, v.boundsVersion);
            }

            size += (long)TypeSizes.sizeof(request.columnFamilies.size());

            String cf;
            for(var4 = request.columnFamilies.iterator(); var4.hasNext(); size += (long)TypeSizes.sizeof(cf)) {
               cf = (String)var4.next();
            }

            return size;
         }
      };
   });
   public final String keyspace;
   public final Collection<Range<Token>> ranges;
   public final Collection<String> columnFamilies = new HashSet();

   public StreamRequest(String keyspace, Collection<Range<Token>> ranges, Collection<String> columnFamilies) {
      this.keyspace = keyspace;
      this.ranges = ranges;
      this.columnFamilies.addAll(columnFamilies);
   }

   public String toString() {
      StringBuilder sb = new StringBuilder("StreamRequest{");
      sb.append("keyspace='").append(this.keyspace).append('\'');
      sb.append(", ranges=").append(this.ranges);
      sb.append(", columnFamilies=").append(this.columnFamilies);
      sb.append('}');
      return sb.toString();
   }
}
