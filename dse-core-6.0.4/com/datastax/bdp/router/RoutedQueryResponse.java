package com.datastax.bdp.router;

import com.datastax.bdp.node.transport.MessageBodySerializer;
import io.netty.buffer.ByteBuf;
import org.apache.cassandra.transport.CBCodec;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;

class RoutedQueryResponse {
   public final ResultMessage resultMessage;
   private static final CBCodec<RoutedQueryResponse> CODEC = new CBCodec<RoutedQueryResponse>() {
      public RoutedQueryResponse decode(ByteBuf body, ProtocolVersion version) {
         ResultMessage resultMessage = (ResultMessage)ResultMessage.codec.decode(body, version);
         return new RoutedQueryResponse(resultMessage);
      }

      public void encode(RoutedQueryResponse msg, ByteBuf dest, ProtocolVersion version) {
         ResultMessage.codec.encode(msg.resultMessage, dest, version);
      }

      public int encodedSize(RoutedQueryResponse msg, ProtocolVersion version) {
         return ResultMessage.codec.encodedSize(msg.resultMessage, version);
      }
   };
   public static final MessageBodySerializer<RoutedQueryResponse> SERIALIZER;

   public RoutedQueryResponse(ResultMessage resultMessage) {
      this.resultMessage = resultMessage;
   }

   static {
      SERIALIZER = new CBCodecBasedSerializer(CODEC);
   }
}
