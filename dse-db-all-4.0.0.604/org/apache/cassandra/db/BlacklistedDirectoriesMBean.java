package org.apache.cassandra.db;

import java.io.File;
import java.util.Set;

public interface BlacklistedDirectoriesMBean {
   Set<File> getUnreadableDirectories();

   Set<File> getUnwritableDirectories();

   void markUnreadable(String var1);

   void markUnwritable(String var1);
}
