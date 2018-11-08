package org.apache.cassandra.db.compaction;

import java.util.List;
import java.util.Map;
import javax.management.openmbean.TabularData;

public interface CompactionManagerMBean {
   List<Map<String, String>> getCompactions();

   List<String> getCompactionSummary();

   TabularData getCompactionHistory();

   void forceUserDefinedCompaction(String var1);

   void forceUserDefinedCleanup(String var1);

   void stopCompaction(String var1);

   void stopCompactionById(String var1);

   int getCoreCompactorThreads();

   void setCoreCompactorThreads(int var1);

   int getMaximumCompactorThreads();

   void setMaximumCompactorThreads(int var1);

   int getCoreValidationThreads();

   void setCoreValidationThreads(int var1);

   int getMaximumValidatorThreads();

   void setMaximumValidatorThreads(int var1);

   int getCoreViewBuildThreads();

   void setCoreViewBuildThreads(int var1);

   int getMaximumViewBuildThreads();

   void setMaximumViewBuildThreads(int var1);
}
