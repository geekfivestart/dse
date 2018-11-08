package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import org.apache.cassandra.transport.ProtocolVersion;

public interface ArgumentDeserializer {
   ArgumentDeserializer NOOP_DESERIALIZER = new ArgumentDeserializer() {
      public Object deserialize(ProtocolVersion protocolVersion, ByteBuffer buffer) {
         return buffer;
      }
   };

   Object deserialize(ProtocolVersion var1, ByteBuffer var2);
}
