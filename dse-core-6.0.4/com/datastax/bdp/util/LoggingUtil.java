package com.datastax.bdp.util;

import ch.qos.logback.classic.LoggerContext;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class LoggingUtil {
   public static final String SERVICE_MDC_KEY = "service";

   public LoggingUtil() {
   }

   public static void setService(String serviceName) {
      MDC.put("service", serviceName);
   }

   public static void resetService() {
      MDC.remove("service");
   }

   public static void stopLoggerContext() {
      try {
         ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
         if(loggerFactory instanceof LoggerContext) {
            ((LoggerContext)loggerFactory).stop();
         }
      } catch (Throwable var1) {
         ;
      }

   }
}
