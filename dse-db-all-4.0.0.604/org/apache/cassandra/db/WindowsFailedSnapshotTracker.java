package org.apache.cassandra.db;

import com.google.common.annotations.VisibleForTesting;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowsFailedSnapshotTracker {
   private static final Logger logger = LoggerFactory.getLogger(WindowsFailedSnapshotTracker.class);
   private static PrintWriter _failedSnapshotFile;
   @VisibleForTesting
   public static final String TODELETEFILE;

   public WindowsFailedSnapshotTracker() {
   }

   public static void deleteOldSnapshots() {
      if(FBUtilities.isWindows) {
         if((new File(TODELETEFILE)).exists()) {
            try {
               BufferedReader reader = new BufferedReader(new FileReader(TODELETEFILE));
               Throwable var1 = null;

               try {
                  String snapshotDirectory;
                  try {
                     while((snapshotDirectory = reader.readLine()) != null) {
                        File f = new File(snapshotDirectory);
                        boolean validFolder = FileUtils.isSubDirectory(new File(System.getenv("TEMP")), f);
                        String[] var5 = DatabaseDescriptor.getAllDataFileLocations();
                        int var6 = var5.length;

                        for(int var7 = 0; var7 < var6; ++var7) {
                           String s = var5[var7];
                           validFolder |= FileUtils.isSubDirectory(new File(s), f);
                        }

                        if(!validFolder) {
                           logger.warn("Skipping invalid directory found in .toDelete: {}. Only %TEMP% or data file subdirectories are valid.", f);
                        } else if(f.exists()) {
                           logger.warn("Discovered obsolete snapshot. Deleting directory [{}]", snapshotDirectory);
                           FileUtils.deleteRecursive(new File(snapshotDirectory));
                        }
                     }
                  } catch (Throwable var19) {
                     var1 = var19;
                     throw var19;
                  }
               } finally {
                  if(reader != null) {
                     if(var1 != null) {
                        try {
                           reader.close();
                        } catch (Throwable var18) {
                           var1.addSuppressed(var18);
                        }
                     } else {
                        reader.close();
                     }
                  }

               }

               Files.delete(Paths.get(TODELETEFILE, new String[0]));
            } catch (IOException var21) {
               logger.warn("Failed to open {}. Obsolete snapshots from previous runs will not be deleted.", TODELETEFILE, var21);
            }
         }

         try {
            _failedSnapshotFile = new PrintWriter(new FileWriter(TODELETEFILE, true));
         } catch (IOException var17) {
            throw new RuntimeException(String.format("Failed to create failed snapshot tracking file [%s]. Aborting", new Object[]{TODELETEFILE}));
         }
      }
   }

   public static synchronized void handleFailedSnapshot(File dir) {
      if(FBUtilities.isWindows) {
         assert _failedSnapshotFile != null : "_failedSnapshotFile not initialized within WindowsFailedSnapshotTracker";

         FileUtils.deleteRecursiveOnExit(dir);
         _failedSnapshotFile.println(dir.toString());
         _failedSnapshotFile.flush();
      }
   }

   @VisibleForTesting
   public static void resetForTests() {
      if(FBUtilities.isWindows) {
         _failedSnapshotFile.close();
      }
   }

   static {
      TODELETEFILE = System.getenv("CASSANDRA_HOME") == null?".toDelete":System.getenv("CASSANDRA_HOME") + File.separator + ".toDelete";
   }
}
