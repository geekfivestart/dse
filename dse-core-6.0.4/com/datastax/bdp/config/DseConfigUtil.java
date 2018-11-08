package com.datastax.bdp.config;

import com.google.common.base.Strings;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Supplier;

public class DseConfigUtil {
   public DseConfigUtil() {
   }

   public static String getAdvancedReplicationDirectory(String advRepDir, Supplier<String> cdcRawDirSupplier) {
      if(!Strings.isNullOrEmpty(advRepDir)) {
         return advRepDir;
      } else {
         String cdcRawDir = (String)cdcRawDirSupplier.get();
         if(!Strings.isNullOrEmpty(cdcRawDir)) {
            Path cdcRawParentPath = Paths.get(cdcRawDir, new String[0]).getParent();
            return cdcRawParentPath.resolve("advrep").toString();
         } else {
            return "/var/lib/cassandra/advrep";
         }
      }
   }
}
