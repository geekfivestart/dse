package com.datastax.bdp.cassandra.auth;

import org.apache.cassandra.auth.IAuthorizer.TransitionalMode;
import org.apache.cassandra.exceptions.ConfigurationException;

public class TransitionalAuthorizer extends DseAuthorizer {
   public TransitionalAuthorizer() {
   }

   public void validateConfiguration() throws ConfigurationException {
      this.enabled = true;
      this.transitionalMode = TransitionalMode.NORMAL;
   }
}
