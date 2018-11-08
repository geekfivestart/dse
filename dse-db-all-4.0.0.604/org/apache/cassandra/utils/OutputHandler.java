package org.apache.cassandra.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface OutputHandler {
   void output(String var1);

   void debug(String var1);

   void warn(String var1);

   void warn(String var1, Throwable var2);

   public static class SystemOutput implements OutputHandler {
      public final boolean debug;
      public final boolean printStack;

      public SystemOutput(boolean debug, boolean printStack) {
         this.debug = debug;
         this.printStack = printStack;
      }

      public void output(String msg) {
         System.out.println(msg);
      }

      public void debug(String msg) {
         if(this.debug) {
            System.out.println(msg);
         }

      }

      public void warn(String msg) {
         this.warn(msg, (Throwable)null);
      }

      public void warn(String msg, Throwable th) {
         System.out.println("WARNING: " + msg);
         if(this.printStack && th != null) {
            th.printStackTrace(System.out);
         }

      }
   }

   public static class LogOutput implements OutputHandler {
      private static Logger logger = LoggerFactory.getLogger(OutputHandler.LogOutput.class);

      public LogOutput() {
      }

      public void output(String msg) {
         logger.info(msg);
      }

      public void debug(String msg) {
         logger.trace(msg);
      }

      public void warn(String msg) {
         logger.warn(msg);
      }

      public void warn(String msg, Throwable th) {
         logger.warn(msg, th);
      }
   }
}
