package com.datastax.bdp.cassandra.auth;

public interface RowLevelAccessControlComponentAuthorizer {
   boolean allowsAccess(String var1, String var2);

   String getComponentName();
}
