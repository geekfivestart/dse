package com.datastax.bdp.router;

import com.datastax.bdp.cassandra.cql3.StatementUtils;
import com.datastax.bdp.node.transport.MessageBodySerializer;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.CBCodec;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.ProtocolVersion;

class RoutedQueryRequest {
   public final String query;
   public final QueryState queryState;
   public final QueryOptions queryOptions;
   public final Map<String, ByteBuffer> payload;
   private static final CBCodec<RoutedQueryRequest> CODEC = new CBCodec<RoutedQueryRequest>() {
      public RoutedQueryRequest decode(ByteBuf body, ProtocolVersion version) {
         String query = CBUtil.readLongString(body);
         QueryOptions queryOptions = (QueryOptions)QueryOptions.codec.decode(body, version);
         ClientState clientState = (ClientState)ClientStateCodec.INSTANCE.decode(body, version);
         Map<String, ByteBuffer> payload = null;
         if(body.readBoolean()) {
            payload = CBUtil.readBytesMap(body);
         }

         QueryState queryState = StatementUtils.queryStateBlocking(clientState);
         return new RoutedQueryRequest(query, queryState, queryOptions, payload);
      }

      public void encode(RoutedQueryRequest msg, ByteBuf dest, ProtocolVersion version) {
         CBUtil.writeLongString(msg.query, dest);
         QueryOptions.codec.encode(msg.queryOptions, dest, version);
         ClientStateCodec.INSTANCE.encode(msg.queryState.getClientState(), dest, version);
         if(msg.payload != null) {
            dest.writeBoolean(true);
            CBUtil.writeBytesMap(msg.payload, dest);
         } else {
            dest.writeBoolean(false);
         }

      }

      public int encodedSize(RoutedQueryRequest msg, ProtocolVersion version) {
         int size = CBUtil.sizeOfLongString(msg.query);
         size += QueryOptions.codec.encodedSize(msg.queryOptions, version);
         size += ClientStateCodec.INSTANCE.encodedSize(msg.queryState.getClientState(), version);
         ++size;
         if(msg.payload != null) {
            size += CBUtil.sizeOfBytesMap(msg.payload);
         }

         return size;
      }
   };
   public static final MessageBodySerializer<RoutedQueryRequest> SERIALIZER;

   public RoutedQueryRequest(String query, QueryState queryState, QueryOptions queryOptions, Map<String, ByteBuffer> payload) {
      this.query = query;
      this.queryState = queryState;
      this.queryOptions = queryOptions;
      this.payload = payload;
   }

   static {
      SERIALIZER = new CBCodecBasedSerializer(CODEC);
   }
}
