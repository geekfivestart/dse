package com.datastax.bdp.server;

import java.io.IOException;
import java.util.List;

public interface DseDaemonMXBean {
   String getReleaseVersion();

   boolean getStartupFinished();

   boolean isKerberosEnabled();

   String getKerberosPrincipal();

   void rebuildSecondaryIndexes(String var1, String var2, List<String> var3) throws IOException;

   List<List<String>> getSplits(String var1, String var2, int var3, String var4, String var5);
}
