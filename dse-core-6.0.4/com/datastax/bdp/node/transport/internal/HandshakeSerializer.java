package com.datastax.bdp.node.transport.internal;

import com.datastax.bdp.node.transport.MessageBodySerializer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class HandshakeSerializer implements MessageBodySerializer<Handshake> {
   public HandshakeSerializer() {
   }

   public void serialize(Handshake h, OutputStream stream) throws IOException {
      stream.write(h.version);
   }

   public Handshake deserialize(InputStream stream) throws IOException {
      return new Handshake((byte)stream.read());
   }
}
