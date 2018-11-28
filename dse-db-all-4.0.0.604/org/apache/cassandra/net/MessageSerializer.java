package org.apache.cassandra.net;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.tracing.Tracing;

class MessageSerializer implements Message.Serializer {
   static final String BASE_TIMESTAMP_KEY = "BASE_TIMESTAMP";
   private final boolean serializeTimestamps = DatabaseDescriptor.hasCrossNodeTimeout();
   private final MessagingVersion version;
   private final long timestampBaseMillis;
   private static final int REQUEST_FLAG = 1;
   private static final int PARAMETERS_FLAG = 2;
   private static final int TIMESTAMP_FLAG = 4;
   private static final int TRACING_FLAG = 8;
   private static final int REQUEST_FORWARDS_FLAG = 16;
   private static final int REQUEST_FORWARDED_FLAG = 32;
   private static final int RESPONSE_FAILURE_FLAG = 16;

   MessageSerializer(MessagingVersion version, long timestampBaseMillis) {
      assert version.isDSE();

      this.version = version;
      this.timestampBaseMillis = timestampBaseMillis;
   }

   public <P> void serialize(Message<P> message, DataOutputPlus out) throws IOException {
      VerbSerializer<?, ?> serializer = this.version.serializer(message.verb());
      out.writeByte(message.group().id().serializationCode());
      out.writeByte(serializer.code);
      out.writeByte(this.computeFlags(message));
      if (!message.verb().isOneWay()) {
         out.writeInt(message.id());
      }
      if (!message.parameters().isEmpty()) {
         MessageParameters.serializer().serialize(message.parameters(), out);
      }
      if (this.serializeTimestamps) {
         out.writeVInt(message.operationStartMillis() - this.timestampBaseMillis);
      }
      if (!message.verb().isOneWay()) {
         out.writeUnsignedVInt(message.timeoutMillis());
      }
      if (message.isTraced()) {
         message.tracingInfo().serialize(out);
      }
      if (message.isRequest()) {
         Request<?,?> request = (Request<?,?>)message;
         if (!request.forwards().isEmpty()) {
            out.writeVInt(request.forwards().size());
            for (Request.Forward forward : request.forwards()) {
               CompactEndpointSerializationHelper.serialize(forward.to, out);
               if (message.verb().isOneWay()) continue;
               out.writeInt(forward.id);
            }
         } else if (request.isForwarded()) {
            CompactEndpointSerializationHelper.serialize(((ForwardRequest)request).replyTo, out);
         }
         ((VerbSerializer)serializer).requestSerializer.serialize(request.payload(), out);
      } else {
         Response response = (Response)message;
         if (response.isFailure()) {
            out.writeInt(((FailureResponse)response).reason().codeForInternodeProtocol(this.version));
         } else {
            ((VerbSerializer)serializer).responseSerializer.serialize(response.payload(), out);
         }
      }
   }


   private int computeFlags(Message message) {
      int flags = 0;
      if(message.isRequest()) {
         flags |= 1;
      }

      if(!message.parameters().isEmpty()) {
         flags |= 2;
      }

      if(this.serializeTimestamps) {
         flags |= 4;
      }

      if(message.isTraced()) {
         flags |= 8;
      }

      if(message.isRequest()) {
         Request<?, ?> request = (Request)message;
         if(!request.forwards().isEmpty()) {
            flags |= 16;
         } else if(request.isForwarded()) {
            flags |= 32;
         }
      } else {
         Response<?> response = (Response)message;
         if(response.isFailure()) {
            flags |= 16;
         }
      }

      return flags;
   }

   public <P> long serializedSize(Message<P> message) {
      long size = 3L;
      if(!message.verb().isOneWay()) {
         size += 4L;
      }

      if(!message.parameters().isEmpty()) {
         size += MessageParameters.serializer().serializedSize(message.parameters());
      }

      if(this.serializeTimestamps) {
         size += (long)TypeSizes.sizeofVInt(message.operationStartMillis() - this.timestampBaseMillis);
      }

      if(!message.verb().isOneWay()) {
         size += (long)TypeSizes.sizeofUnsignedVInt(message.timeoutMillis());
      }

      if(message.isTraced()) {
         size += message.tracingInfo().serializedSize();
      }

      VerbSerializer serializer;
      if(message.isRequest()) {
         Request<P, ?> request = (Request)message;
         serializer = this.version.serializer(request.verb());
         if(!request.forwards().isEmpty()) {
            size += (long)TypeSizes.sizeofVInt((long)request.forwards().size());
            Iterator var6 = request.forwards().iterator();

            while(var6.hasNext()) {
               Request.Forward forward = (Request.Forward)var6.next();
               size += (long)CompactEndpointSerializationHelper.serializedSize(forward.to);
               if(!message.verb().isOneWay()) {
                  size += 4L;
               }
            }
         } else if(request.isForwarded()) {
            size += (long)CompactEndpointSerializationHelper.serializedSize(((ForwardRequest)request).replyTo);
         }

         size += serializer.requestSerializer.serializedSize(request.payload());
      } else {
         Response<P> response = (Response)message;
         serializer = this.version.serializer(response.verb());
         if(response.isFailure()) {
            size += 4L;
         } else {
            size += serializer.responseSerializer.serializedSize(response.payload());
         }
      }

      return size;
   }

   public <P> Message<P> deserialize(TrackedDataInputPlus in, int size, InetAddress from) throws IOException {
      long startBytes = in.getBytesRead();
      int groupCode = in.readUnsignedByte();
      int verbCode = in.readUnsignedByte();
      int flags = in.readUnsignedByte();
      VerbGroup<?> group = Verbs.fromSerializationCode(groupCode);
      VerbSerializer<?, ?> serializer = this.version.serializerByVerbCode(group, verbCode);
      int messageId = serializer.verb.isOneWay()?-1:in.readInt();
      boolean hasParameters = (flags & 2) != 0;
      MessageParameters parameters = hasParameters?MessageParameters.serializer().deserialize(in):MessageParameters.EMPTY;
      long createdAtMillis = System.currentTimeMillis();
      long timeoutMillis;
      if(this.serializeTimestamps) {
         timeoutMillis = this.timestampBaseMillis + in.readVInt();
         long elapsed = createdAtMillis - timeoutMillis;
         if(elapsed > 0L) {
            MessagingService.instance().metrics.addTimeTaken(from, createdAtMillis - timeoutMillis);
            createdAtMillis = timeoutMillis + DatabaseDescriptor.getEndpointSnitch().getCrossDcRttLatency(from) / 2L;
         }
      }

      timeoutMillis = serializer.verb.isOneWay()?-1L:in.readUnsignedVInt();
      Tracing.SessionInfo tracingInfo = null;
      if((flags & 8) != 0) {
         tracingInfo = Tracing.SessionInfo.deserialize(in);
      }

      Verb verb;
      Object forwards;
      if((flags & 1) == 0) {
         verb = serializer.verb;
         Message.Data data;
         if((flags & 16) != 0) {
            RequestFailureReason reason = RequestFailureReason.fromCode(in.readInt());
            data = new Message.Data((Object)null, -1L, createdAtMillis, timeoutMillis, parameters, tracingInfo);
            return new FailureResponse(from, Request.local, messageId, verb, reason, data);
         } else {
            try {
               forwards = serializer.responseSerializer.deserialize(in);
               data = new Message.Data(forwards, -1L, createdAtMillis, timeoutMillis, parameters, tracingInfo);
               return new Response(from, Request.local, messageId, verb, data);
            } catch (Exception var27) {
               int remainingBytes = size - (int)(in.getBytesRead() - startBytes);
               throw MessageDeserializationException.forResponsePayloadDeserializationException(var27, from, verb, remainingBytes);
            }
         }
      } else {
         verb = serializer.verb;
         forwards = Collections.emptyList();
         InetAddress replyTo = null;
         int remainingBytes;
         if((flags & 16) != 0) {
            int forwardCount = (int)in.readVInt();
            forwards = new ArrayList(forwardCount);

            for(remainingBytes = 0; remainingBytes < forwardCount; ++remainingBytes) {
               InetAddress addr = CompactEndpointSerializationHelper.deserialize(in);
               int id = verb.isOneWay()?-1:in.readInt();
               ((List)forwards).add(new Request.Forward(addr, id));
            }
         } else if((flags & 32) != 0) {
            replyTo = CompactEndpointSerializationHelper.deserialize(in);
         }

         try {
            P payload = (P)serializer.requestSerializer.deserialize(in);
            Message.Data<P> data = new Message.Data(payload, -1L, createdAtMillis, timeoutMillis, parameters, tracingInfo);
            return (Message)(replyTo == null?(verb.isOneWay()?new OneWayRequest(from, Request.local, (Verb.OneWay)verb, data, (List)forwards):new Request(from, Request.local, messageId, verb, data, (List)forwards)):new ForwardRequest(from, Request.local, replyTo, messageId, verb, data));
         } catch (Exception var26) {
            remainingBytes = size - (int)(in.getBytesRead() - startBytes);
            throw MessageDeserializationException.forRequestPayloadDeserializationException(var26, from, messageId, verb, createdAtMillis, timeoutMillis, parameters, tracingInfo, remainingBytes);
         }
      }
   }
}
