package com.datastax.bdp.util.process;

import ch.qos.logback.classic.Level;
import com.datastax.bdp.server.DseDaemon;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

public class ProcessUtil {
   private static Logger logger = LoggerFactory.getLogger(ProcessUtil.class);

   public ProcessUtil() {
   }

   public static void logProcessOutStream(Process process, Marker marker, String name, String logFormat) {
      ProcessOutputStreamProcessor stdOutLogger = new ExternalLogger(marker, logFormat, Level.INFO, name);
      ProcessUtil.IOReaper reaper = new ProcessUtil.IOReaper(process.getInputStream(), stdOutLogger);
      reaper.setName(name + " logger");
      reaper.start();
   }

   public static void logProcessErrStream(Process process, Marker marker, String name, String logFormat) {
      ProcessOutputStreamProcessor stdErrLogger = new ExternalLogger(marker, logFormat, Level.ERROR, name);
      ProcessUtil.IOReaper reaper = new ProcessUtil.IOReaper(process.getErrorStream(), stdErrLogger);
      reaper.setName(name + " error logger");
      reaper.start();
   }

   public static class IOReaper extends Thread {
      private final BufferedReader reader;
      private final ProcessOutputStreamProcessor extLogger;

      IOReaper(InputStream is, ProcessOutputStreamProcessor extLogger) {
         this.reader = new BufferedReader(new InputStreamReader(is));
         this.extLogger = extLogger;
         this.setDaemon(true);
      }

      public void run() {
         while(true) {
            try {
               String line;
               if((line = this.reader.readLine()) != null) {
                  this.extLogger.processLine(line);
                  continue;
               }
            } catch (IOException var3) {
               if(!DseDaemon.isStopped()) {
                  ProcessUtil.logger.error("IOReaper failure", var3);
               } else {
                  ProcessUtil.logger.debug("IOReaper failure during node shutdown", var3);
               }
            }

            return;
         }
      }
   }
}
