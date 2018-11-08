package com.datastax.bdp.node.transport.internal;

import com.datastax.bdp.node.transport.MessageBodySerializer;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class FailedProcessorSerializer implements MessageBodySerializer<FailedProcessorException> {
   public FailedProcessorSerializer() {
   }

   public void serialize(FailedProcessorException body, OutputStream stream) throws IOException {
      DataOutputStream out = new DataOutputStream(stream);
      out.writeUTF(body.errorClass.getCanonicalName());
      if(body.getMessage() != null) {
         out.writeUTF(body.getMessage());
      } else {
         out.writeUTF("");
      }

   }

   public FailedProcessorException deserialize(InputStream stream) throws IOException {
      DataInputStream in = new DataInputStream(stream);
      String errorClassName = in.readUTF();
      String errorMessage = in.readUTF();

      Class errorClass;
      try {
         errorClass = Class.forName(errorClassName);
      } catch (Exception var7) {
         errorClass = Throwable.class;
      }

      return new FailedProcessorException(errorClass, errorMessage);
   }
}
