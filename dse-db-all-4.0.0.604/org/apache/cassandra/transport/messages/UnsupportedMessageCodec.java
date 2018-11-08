package org.apache.cassandra.transport.messages;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnsupportedMessageCodec<T extends Message> implements Message.Codec<T> {
   public static final UnsupportedMessageCodec instance = new UnsupportedMessageCodec();
   private static final Logger logger = LoggerFactory.getLogger(UnsupportedMessageCodec.class);

   public UnsupportedMessageCodec() {
   }

   public T decode(ByteBuf body, ProtocolVersion version) {
      if(ProtocolVersion.SUPPORTED.contains(version)) {
         logger.error("Received invalid message for supported protocol version {}", version);
      }

      throw new ProtocolException("Unsupported message");
   }

   public void encode(T t, ByteBuf dest, ProtocolVersion version) {
      throw new ProtocolException("Unsupported message");
   }

   public int encodedSize(T t, ProtocolVersion version) {
      return 0;
   }
}
