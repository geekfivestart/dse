package com.datastax.bdp.cassandra.auth;

public interface LdapUtilsMXBean {
   long getSearchCacheSize();

   long getCredentialsCacheSize();

   long getConnectionPoolActive();

   long getConnectionPoolIdle();

   void invalidateSearchCacheAll();

   void invalidateSearchCache(String var1);

   void invalidateCredentialsCacheAll();

   void invalidateCredentialsCache(String var1);
}
