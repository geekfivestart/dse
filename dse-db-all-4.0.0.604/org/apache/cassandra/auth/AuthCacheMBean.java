package org.apache.cassandra.auth;

import java.util.Map;

public interface AuthCacheMBean {
   void invalidate();

   void invalidateAll();

   void setValidity(int var1);

   int getValidity();

   void setUpdateInterval(int var1);

   int getUpdateInterval();

   void setMaxEntries(int var1);

   int getMaxEntries();

   void setInitialCapacity(int var1);

   int getInitialCapacity();

   Map<String, Number> getCacheStats();
}
