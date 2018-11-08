package com.datastax.bdp.cassandra.auth;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.permission.Permissions;

public enum ProxyPermission implements Permission {
   EXECUTE,
   LOGIN;

   private ProxyPermission() {
   }

   public String domain() {
      return "PROXY";
   }

   static {
      Permissions.register("PROXY", ProxyPermission.class);
   }
}
