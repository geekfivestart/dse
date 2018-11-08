package org.apache.cassandra.streaming;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

public interface StreamConnectionFactory {
   Socket createConnection(InetAddress var1) throws IOException;
}
