package org.apache.cassandra.exceptions;

public class ConfigurationException extends RequestValidationException {
   public final boolean logStackTrace;

   public ConfigurationException(String msg) {
      super(ExceptionCode.CONFIG_ERROR, msg);
      this.logStackTrace = true;
   }

   public ConfigurationException(String msg, boolean logStackTrace) {
      super(ExceptionCode.CONFIG_ERROR, msg);
      this.logStackTrace = logStackTrace;
   }

   public ConfigurationException(String msg, Throwable e) {
      super(ExceptionCode.CONFIG_ERROR, msg, e);
      this.logStackTrace = true;
   }

   protected ConfigurationException(ExceptionCode code, String msg) {
      super(code, msg);
      this.logStackTrace = true;
   }
}
