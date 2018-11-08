package org.apache.cassandra.net.interceptors;

public interface AbstractInterceptorMBean {
   void enable();

   void disable();

   boolean getEnabled();

   long getSeenCount();

   long getInterceptedCount();

   String getIntercepted();

   void setIntercepted(String var1);

   String getInterceptedDirections();

   void setInterceptedDirections(String var1);

   String getInterceptedTypes();

   void setInterceptedTypes(String var1);

   String getInterceptedLocalities();

   void setInterceptedLocalities(String var1);

   float getInterceptionChance();

   void setInterceptionChance(float var1);
}
