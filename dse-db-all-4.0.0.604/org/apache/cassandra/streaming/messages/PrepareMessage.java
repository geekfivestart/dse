package org.apache.cassandra.streaming.messages;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.streaming.StreamRequest;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamSummary;
import org.apache.cassandra.utils.Serializer;

public class PrepareMessage extends StreamMessage {
   public static StreamMessage.Serializer<PrepareMessage> serializer = new StreamMessage.Serializer<PrepareMessage>() {
      public PrepareMessage deserialize(ReadableByteChannel in, StreamMessage.StreamVersion version, StreamSession session) throws IOException {
         DataInputPlus input = new DataInputPlus.DataInputStreamPlus(Channels.newInputStream(in));
         PrepareMessage message = new PrepareMessage();
         int numRequests = input.readInt();

         int numSummaries;
         for(numSummaries = 0; numSummaries < numRequests; ++numSummaries) {
            message.requests.add((StreamRequest.serializers.get(version)).deserialize(input));
         }

         numSummaries = input.readInt();

         for(int i = 0; i < numSummaries; ++i) {
            message.summaries.add((StreamSummary.serializers.get(version)).deserialize(input));
         }

         return message;
      }

      public void serialize(PrepareMessage message, DataOutputStreamPlus out, StreamMessage.StreamVersion version, StreamSession session) throws IOException {
         out.writeInt(message.requests.size());
         for (StreamRequest request : message.requests) {
            StreamRequest.serializers.get(version).serialize(request, out);
         }
         out.writeInt(message.summaries.size());
         for (StreamSummary summary : message.summaries) {
            StreamSummary.serializers.get(version).serialize(summary, out);
         }
      }
   };
   public final Collection<StreamRequest> requests = new ArrayList();
   public final Collection<StreamSummary> summaries = new ArrayList();

   public PrepareMessage() {
      super(StreamMessage.Type.PREPARE);
   }

   public String toString() {
      StringBuilder sb = new StringBuilder("Prepare (");
      sb.append(this.requests.size()).append(" requests:[");
      boolean first = true;

      StreamRequest request;
      for(Iterator var3 = this.requests.iterator(); var3.hasNext(); sb.append(request)) {
         request = (StreamRequest)var3.next();
         if(first) {
            first = false;
         } else {
            sb.append(',');
         }
      }

      sb.append("], ");
      int totalFile = 0;
      long totalSize = 0L;

      Iterator var6;
      StreamSummary summary;
      for(var6 = this.summaries.iterator(); var6.hasNext(); totalSize += summary.totalSize) {
         summary = (StreamSummary)var6.next();
         totalFile += summary.files;
      }

      sb.append(" ").append(totalFile).append(" files, ");
      sb.append(" ").append(totalSize).append(" bytes, summaries:[");
      first = true;

      for(var6 = this.summaries.iterator(); var6.hasNext(); sb.append(summary.toString())) {
         summary = (StreamSummary)var6.next();
         if(first) {
            first = false;
         } else {
            sb.append(',');
         }
      }

      sb.append("]}");
      return sb.toString();
   }
}
