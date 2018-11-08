package com.datastax.bdp.router;

import com.datastax.bdp.node.transport.MessageBodySerializer;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.CBCodec;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.ProtocolVersion;

class RoutedRpcRequest {
   public final String rpcObjectName;
   public final String rpcMethodName;
   public final ClientState clientState;
   public final List<ByteBuffer> values;
   private static final CBCodec<RoutedRpcRequest> CODEC = new CBCodec<RoutedRpcRequest>() {
      public RoutedRpcRequest decode(ByteBuf body, ProtocolVersion version) {
         String rpcObjectName = CBUtil.readLongString(body);
         String rpcMethodName = CBUtil.readLongString(body);
         ClientState clientState = (ClientState)ClientStateCodec.INSTANCE.decode(body, version);
         List<ByteBuffer> values = CBUtil.readValueList(body, version);
         return new RoutedRpcRequest(rpcObjectName, rpcMethodName, clientState, values);
      }

      public void encode(RoutedRpcRequest msg, ByteBuf dest, ProtocolVersion version) {
         CBUtil.writeLongString(msg.rpcObjectName, dest);
         CBUtil.writeLongString(msg.rpcMethodName, dest);
         ClientStateCodec.INSTANCE.encode(msg.clientState, dest, version);
         CBUtil.writeValueList(msg.values, dest);
      }

      public int encodedSize(RoutedRpcRequest msg, ProtocolVersion version) {
         int size = CBUtil.sizeOfLongString(msg.rpcObjectName);
         size += CBUtil.sizeOfLongString(msg.rpcMethodName);
         size += ClientStateCodec.INSTANCE.encodedSize(msg.clientState, version);
         size += CBUtil.sizeOfValueList(msg.values);
         return size;
      }
   };
   public static final MessageBodySerializer<RoutedRpcRequest> SERIALIZER;

   public RoutedRpcRequest(String rpcObjectName, String rpcMethodName, ClientState clientState, List<ByteBuffer> values) {
      this.rpcObjectName = rpcObjectName;
      this.rpcMethodName = rpcMethodName;
      this.clientState = clientState;
      this.values = values;
   }

   static {
      SERIALIZER = new CBCodecBasedSerializer(CODEC);
   }
}
