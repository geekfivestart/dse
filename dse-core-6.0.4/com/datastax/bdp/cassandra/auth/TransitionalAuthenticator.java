package com.datastax.bdp.cassandra.auth;

import org.apache.cassandra.auth.IAuthenticator.TransitionalMode;
import org.apache.cassandra.exceptions.ConfigurationException;

public class TransitionalAuthenticator extends DseAuthenticator {
   public TransitionalAuthenticator() {
      super(true);
   }

   public void validateConfiguration() throws ConfigurationException {
      this.defaultScheme = AuthenticationScheme.INTERNAL;
      this.allowedSchemes.add(this.defaultScheme);
      this.transitionalMode = TransitionalMode.NORMAL;
   }
}
