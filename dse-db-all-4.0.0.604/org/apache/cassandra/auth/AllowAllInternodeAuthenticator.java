package org.apache.cassandra.auth;

import java.net.InetAddress;
import org.apache.cassandra.exceptions.ConfigurationException;

public class AllowAllInternodeAuthenticator implements IInternodeAuthenticator {
   public AllowAllInternodeAuthenticator() {
   }

   public boolean authenticate(InetAddress remoteAddress, int remotePort) {
      return true;
   }

   public void validateConfiguration() throws ConfigurationException {
   }
}
