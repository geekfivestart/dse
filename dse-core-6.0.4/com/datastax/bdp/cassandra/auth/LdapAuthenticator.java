package com.datastax.bdp.cassandra.auth;

import org.apache.cassandra.exceptions.ConfigurationException;

public class LdapAuthenticator extends DseAuthenticator {
   public LdapAuthenticator() {
      super(true);
   }

   public void validateConfiguration() throws ConfigurationException {
      this.defaultScheme = AuthenticationScheme.LDAP;
      this.allowedSchemes.add(this.defaultScheme);
      LdapUtils.instance.validateAuthenticationConfiguration();
   }
}
