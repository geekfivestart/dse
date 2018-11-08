package com.datastax.bdp.node.transport;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.cassandra.utils.UUIDGen;

public abstract class TraceAwareSerializer<T extends TraceAwareMessage> implements MessageBodySerializer<T> {
   public TraceAwareSerializer() {
   }

   public T deserialize(InputStream in) throws IOException {
      DataInputStream inputStream = new DataInputStream(in);
      boolean tracing = inputStream.readBoolean();
      String source = null;
      UUID session = null;
      String type = null;
      if(tracing) {
         source = inputStream.readUTF();
         int size = inputStream.readInt();
         byte[] uuid = new byte[size];
         inputStream.read(uuid);
         session = UUIDGen.getUUID(ByteBuffer.wrap(uuid));
         type = inputStream.readUTF();
      }

      T message = this.deserializeBody(in);
      if(tracing) {
         message.setTraceState(source, session, type);
      }

      return message;
   }

   public void serialize(T message, OutputStream out) throws IOException {
      DataOutputStream outputStream = new DataOutputStream(out);
      outputStream.writeBoolean(message.hasTraceState());
      if(message.hasTraceState()) {
         outputStream.writeUTF(message.getSource());
         byte[] uuid = UUIDGen.decompose(message.getSession());
         outputStream.writeInt(uuid.length);
         outputStream.write(uuid);
         outputStream.writeUTF(message.getType());
      }

      this.serializeBody(message, out);
   }

   protected abstract T deserializeBody(InputStream var1) throws IOException;

   protected abstract void serializeBody(T var1, OutputStream var2) throws IOException;
}
