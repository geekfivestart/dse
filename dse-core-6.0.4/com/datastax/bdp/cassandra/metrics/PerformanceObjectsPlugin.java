package com.datastax.bdp.cassandra.metrics;

import com.datastax.bdp.plugin.AbstractPlugin;
import com.datastax.bdp.plugin.DsePlugin;
import com.datastax.bdp.server.SystemInfo;
import com.datastax.bdp.system.PerformanceObjectsKeyspace;
import com.google.inject.Singleton;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@DsePlugin(
   dependsOn = {}
)
public class PerformanceObjectsPlugin extends AbstractPlugin {
   public static final Logger logger = LoggerFactory.getLogger(PerformanceObjectsPlugin.class);
   private static final String BASE_OPTIONS = " CACHING = {'keys':'NONE', 'rows_per_partition':'NONE'}";
   public static final String COMP_STRATEGY = " AND COMPACTION = {'class':'TimeWindowCompactionStrategy'}";
   private static final Set<String> untrackedKeyspaces = Collections.newSetFromMap(new ConcurrentHashMap());

   public PerformanceObjectsPlugin() {
      untrackedKeyspaces.addAll(SystemInfo.SYSTEM_KEYSPACES);
      untrackedKeyspaces.add("dse_perf");
   }

   public static String getAdditionalTableOptions() {
      return " CACHING = {'keys':'NONE', 'rows_per_partition':'NONE'} AND COMPACTION = {'class':'TimeWindowCompactionStrategy'}";
   }

   public void setupSchema() {
      PerformanceObjectsKeyspace.init();
      PerformanceObjectsKeyspace.maybeConfigure();
   }

   public static boolean isUntracked(String keyspaceName) {
      return null == keyspaceName || untrackedKeyspaces.contains(keyspaceName);
   }

   public static void addUntracked(String... keyspaceNames) {
      Collections.addAll(untrackedKeyspaces, keyspaceNames);
   }
}
