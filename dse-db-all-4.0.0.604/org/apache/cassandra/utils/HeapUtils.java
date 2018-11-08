package org.apache.cassandra.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.text.StrBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HeapUtils {
   private static final Logger logger = LoggerFactory.getLogger(HeapUtils.class);

   public static void logHeapHistogram() {
      try {
         logger.info("Trying to log the heap histogram using jcmd");
         Long processId = getProcessId();
         if(processId == null) {
            logger.error("The process ID could not be retrieved. Skipping heap histogram generation.");
            return;
         }

         String jcmdPath = getJcmdPath();
         String jcmdCommand = jcmdPath == null?"jcmd":jcmdPath;
         String[] histoCommands = new String[]{jcmdCommand, processId.toString(), "GC.class_histogram"};
         logProcessOutput(Runtime.getRuntime().exec(histoCommands));
      } catch (Throwable var4) {
         logger.error("The heap histogram could not be generated due to the following error: ", var4);
      }

   }

   private static String getJcmdPath() {
      String javaHome = System.getenv("JAVA_HOME");
      if(javaHome == null) {
         return null;
      } else {
         File javaBinDirectory = new File(javaHome, "bin");
         File[] files = javaBinDirectory.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
               return name.startsWith("jcmd");
            }
         });
         return ArrayUtils.isEmpty(files)?null:files[0].getPath();
      }
   }

   private static void logProcessOutput(Process p) throws IOException {
      BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
      Throwable var2 = null;

      try {
         StrBuilder builder = new StrBuilder();

         String line;
         while((line = input.readLine()) != null) {
            builder.appendln(line);
         }

         logger.info(builder.toString());
      } catch (Throwable var12) {
         var2 = var12;
         throw var12;
      } finally {
         if(input != null) {
            if(var2 != null) {
               try {
                  input.close();
               } catch (Throwable var11) {
                  var2.addSuppressed(var11);
               }
            } else {
               input.close();
            }
         }

      }
   }

   private static Long getProcessId() {
      long pid = NativeLibrary.getProcessID();
      return pid >= 0L?Long.valueOf(pid):getProcessIdFromJvmName();
   }

   private static Long getProcessIdFromJvmName() {
      String jvmName = ManagementFactory.getRuntimeMXBean().getName();

      try {
         return Long.valueOf(jvmName.split("@")[0]);
      } catch (NumberFormatException var2) {
         return null;
      }
   }

   private HeapUtils() {
   }
}
