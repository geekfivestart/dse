package org.apache.cassandra.hints;

public interface HintsServiceMBean {
   void pauseDispatch();

   void resumeDispatch();

   void deleteAllHints();

   void deleteAllHintsForEndpoint(String var1);
}
