package com.datastax.bdp.cassandra.auth;

import org.apache.cassandra.exceptions.ConfigurationException;

public class LegacyKerberosAuthenticator extends DseAuthenticator {
   public LegacyKerberosAuthenticator() {
      super(true);
   }

   public void validateConfiguration() throws ConfigurationException {
      this.validateKeytab();
      this.defaultScheme = AuthenticationScheme.KERBEROS;
      this.allowedSchemes.add(this.defaultScheme);
   }
}
