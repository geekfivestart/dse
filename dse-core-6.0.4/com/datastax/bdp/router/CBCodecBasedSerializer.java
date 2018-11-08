package com.datastax.bdp.router;

import com.datastax.bdp.node.transport.MessageBodySerializer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.cassandra.transport.CBCodec;
import org.apache.cassandra.transport.ProtocolVersion;

class CBCodecBasedSerializer<T> implements MessageBodySerializer<T> {
   private final CBCodec<T> codec;
   private static final ProtocolVersion SERVER_VERSION;

   public CBCodecBasedSerializer(CBCodec<T> codec) {
      this.codec = codec;
   }

   public void serialize(T body, OutputStream stream) throws IOException {
      assert stream instanceof ByteBufOutputStream;

      ByteBuf buf = ((ByteBufOutputStream)stream).buffer();
      int size = this.codec.encodedSize(body, SERVER_VERSION);
      buf.writeInt(size);
      this.codec.encode(body, buf, SERVER_VERSION);
   }

   public T deserialize(InputStream stream) throws IOException {
      assert stream instanceof ByteBufInputStream;

      int size = ((ByteBufInputStream)stream).readInt();
      ByteBuf buf = PooledByteBufAllocator.DEFAULT.heapBuffer(size, size);

      Object var4;
      try {
         buf.writeBytes(stream, size);
         var4 = this.codec.decode(buf, SERVER_VERSION);
      } finally {
         buf.release();
      }

      return var4;
   }

   static {
      SERVER_VERSION = ProtocolVersion.V4;
   }
}
