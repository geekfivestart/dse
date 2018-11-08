package com.datastax.bdp.node.transport.internal;

import com.datastax.bdp.node.transport.MessageBodySerializer;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class UnsupportedMessageSerializer implements MessageBodySerializer<UnsupportedMessageException> {
   public UnsupportedMessageSerializer() {
   }

   public void serialize(UnsupportedMessageException ex, OutputStream stream) throws IOException {
      DataOutputStream dataOutputStream = new DataOutputStream(stream);
      dataOutputStream.writeUTF(ex.getMessage());
   }

   public UnsupportedMessageException deserialize(InputStream stream) throws IOException {
      DataInputStream dataInputStream = new DataInputStream(stream);
      return new UnsupportedMessageException(dataInputStream.readUTF());
   }
}
