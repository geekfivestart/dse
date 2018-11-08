package org.apache.cassandra.transport;

import io.netty.buffer.ByteBuf;

public interface CBCodec<T> {
   T decode(ByteBuf var1, ProtocolVersion var2);

   void encode(T var1, ByteBuf var2, ProtocolVersion var3);

   int encodedSize(T var1, ProtocolVersion var2);
}
