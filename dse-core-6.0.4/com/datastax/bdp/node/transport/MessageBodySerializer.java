package com.datastax.bdp.node.transport;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface MessageBodySerializer<T> {
   void serialize(T var1, OutputStream var2) throws IOException;

   T deserialize(InputStream var1) throws IOException;
}
