package com.datastax.bdp.util;

public enum HadoopVersion {
   HADOOP2("HADOOP2_CONF_DIR", new String[]{"$HADOOP2_HOME/conf", "$DSE_HOME/resources/hadoop2-client/conf", "/etc/dse/hadoop2-client", "/usr/share/dse/hadoop2-client/conf"});

   private final String envVar;
   private final String[] dirs;

   private HadoopVersion(String envVar, String[] dirs) {
      this.envVar = envVar;
      this.dirs = dirs;
   }

   public String getEnvVar() {
      return this.envVar;
   }

   public String[] getAllDirNames() {
      return this.dirs;
   }
}
