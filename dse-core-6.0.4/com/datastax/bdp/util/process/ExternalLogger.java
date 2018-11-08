package com.datastax.bdp.util.process;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import java.text.MessageFormat;
import java.util.Date;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

public class ExternalLogger implements ProcessOutputStreamProcessor {
   private final Marker marker;
   private final Level defaultLevel;
   private final String defaultClassName;
   private final MessageFormat mf;
   private final Object[] NO_ARGS = new Object[0];
   private ExternalLogger.LogInfo last = null;

   public ExternalLogger(Marker marker, String messageFormat, Level defaultLevel, String defaultClassName) {
      this.marker = marker;
      this.mf = new MessageFormat(messageFormat);
      this.defaultLevel = defaultLevel;
      this.defaultClassName = defaultClassName;
   }

   public void processLine(String line) {
      ExternalLogger.LogInfo logInfo = this.parseLogInfo(line);
      if(logInfo != null) {
         this.log(logInfo);
         this.last = logInfo;
      } else if(this.last != null) {
         this.log(this.last.withMessage(line));
      } else {
         this.log(this.newPlainLogInfo(this.defaultLevel, this.defaultClassName, line));
      }

   }

   protected void log(ExternalLogger.LogInfo logInfo) {
      Logger logger = (Logger)LoggerFactory.getLogger(logInfo.className);
      if(logInfo.level == Level.TRACE) {
         logger.trace(this.marker, logInfo.message, this.NO_ARGS);
      } else if(logInfo.level == Level.DEBUG) {
         logger.debug(this.marker, logInfo.message, this.NO_ARGS);
      } else if(logInfo.level == Level.INFO) {
         logger.info(this.marker, logInfo.message, this.NO_ARGS);
      } else if(logInfo.level == Level.WARN) {
         logger.warn(this.marker, logInfo.message, this.NO_ARGS);
      } else {
         if(logInfo.level != Level.ERROR) {
            throw new IllegalArgumentException("Invalid log level");
         }

         logger.error(this.marker, logInfo.message, this.NO_ARGS);
      }

   }

   private ExternalLogger.LogInfo parseLogInfo(String line) {
      try {
         Object[] args = this.mf.parse(line);
         return new ExternalLogger.LogInfo(this.toLevel((String)args[0], Level.DEBUG), (Date)args[1], (String)args[2], (String)args[3]);
      } catch (Exception var3) {
         return null;
      }
   }

   private Level toLevel(String name, Level defaultLevel) {
      byte var4 = -1;
      switch(name.hashCode()) {
      case 2251950:
         if(name.equals("INFO")) {
            var4 = 2;
         }
         break;
      case 2656902:
         if(name.equals("WARN")) {
            var4 = 3;
         }
         break;
      case 64921139:
         if(name.equals("DEBUG")) {
            var4 = 1;
         }
         break;
      case 66247144:
         if(name.equals("ERROR")) {
            var4 = 4;
         }
         break;
      case 66665700:
         if(name.equals("FATAL")) {
            var4 = 5;
         }
         break;
      case 80083237:
         if(name.equals("TRACE")) {
            var4 = 0;
         }
      }

      switch(var4) {
      case 0:
         return Level.TRACE;
      case 1:
         return Level.DEBUG;
      case 2:
         return Level.INFO;
      case 3:
         return Level.WARN;
      case 4:
         return Level.ERROR;
      case 5:
         return Level.ERROR;
      default:
         return defaultLevel;
      }
   }

   private ExternalLogger.LogInfo newPlainLogInfo(Level level, String className, String message) {
      return new ExternalLogger.LogInfo(level, new Date(), className, message);
   }

   static class LogInfo {
      final Level level;
      final Date date;
      final String className;
      final String message;

      private LogInfo(Level level, Date date, String className, String message) {
         this.level = level;
         this.date = date;
         this.className = className;
         this.message = message;
      }

      public ExternalLogger.LogInfo withMessage(String newMessage) {
         return new ExternalLogger.LogInfo(this.level, this.date, this.className, newMessage);
      }
   }
}
