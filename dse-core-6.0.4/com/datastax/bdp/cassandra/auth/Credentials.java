package com.datastax.bdp.cassandra.auth;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class Credentials {
   public final String authenticationUser;
   public final String authorizationUser;
   public final String password;

   public Credentials(String user, String password) {
      this(user, password, (String)null);
   }

   public Credentials(String authenticationUser, String password, String authorizationUser) {
      this.authenticationUser = authenticationUser;
      this.authorizationUser = authorizationUser;
      this.password = password;
   }

   public Credentials(Map<String, String> credMap) {
      this((String)credMap.get("username"), (String)credMap.get("password"), (String)null);
   }

   public Map<String, String> toMap() {
      Map<String, String> credMap = new HashMap();
      if(!StringUtils.isEmpty(this.authenticationUser)) {
         credMap.put("username", this.authenticationUser);
      }

      if(!StringUtils.isEmpty(this.password)) {
         credMap.put("password", this.password);
      }

      return credMap;
   }

   public String toString() {
      return String.format("Credentials: authenticate as %s, authorize as %s, password %s)", new Object[]{this.authenticationUser, this.authorizationUser, this.password});
   }
}
