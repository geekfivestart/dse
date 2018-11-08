package org.apache.cassandra.auth;

import java.net.InetAddress;
import org.apache.cassandra.exceptions.ConfigurationException;

public interface IInternodeAuthenticator {
   boolean authenticate(InetAddress var1, int var2);

   void validateConfiguration() throws ConfigurationException;
}
