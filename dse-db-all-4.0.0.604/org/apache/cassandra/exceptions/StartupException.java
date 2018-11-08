package org.apache.cassandra.exceptions;

public class StartupException extends Exception {
   public static final int ERR_WRONG_MACHINE_STATE = 1;
   public static final int ERR_WRONG_DISK_STATE = 3;
   public static final int ERR_WRONG_CONFIG = 100;
   public static final int ERR_OUTDATED_SCHEMA = 101;
   public final int returnCode;

   public StartupException(int returnCode, String message) {
      super(message);
      this.returnCode = returnCode;
   }

   public StartupException(int returnCode, String message, Throwable cause) {
      super(message, cause);
      this.returnCode = returnCode;
   }
}
