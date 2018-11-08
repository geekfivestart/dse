package com.datastax.bdp.cassandra.auth;

import com.datastax.bdp.config.DseConfig;
import org.apache.cassandra.exceptions.ConfigurationException;

public class KerberosAuthenticator extends DseAuthenticator {
   public KerberosAuthenticator() {
      super(true);
   }

   public void validateConfiguration() throws ConfigurationException {
      this.validateKeytab();
      this.defaultScheme = AuthenticationScheme.KERBEROS;
      this.allowedSchemes.add(this.defaultScheme);
      if(DseConfig.isAllowDigestWithKerberos()) {
         this.allowedSchemes.add(AuthenticationScheme.TOKEN);
      }

   }
}
