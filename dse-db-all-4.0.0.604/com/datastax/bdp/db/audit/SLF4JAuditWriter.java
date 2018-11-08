package com.datastax.bdp.db.audit;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import io.reactivex.Completable;
import java.util.Iterator;
import org.apache.cassandra.utils.WrappedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SLF4JAuditWriter implements IAuditWriter {
   private static final Logger logger = LoggerFactory.getLogger(SLF4JAuditWriter.class);
   public static final String LOGGER_NAME = "SLF4JAuditWriter";
   private static final Logger AUDIT_LOG = LoggerFactory.getLogger("SLF4JAuditWriter");

   public SLF4JAuditWriter() {
      Logger rootLogger = LoggerFactory.getLogger("ROOT");
      if(rootLogger == logger) {
         logger.debug("Audit logging disabled (should not use root logger)");
      } else {
         if(AUDIT_LOG instanceof ch.qos.logback.classic.Logger) {
            final ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger)AUDIT_LOG;
            if(!logbackLogger.isAdditive()) {
               logger.debug(String.format("Audit logging disabled (turn off additivity for audit logger %s)", new Object[]{"SLF4JAuditWriter"}));
               return;
            }

            Thread logFlusher = new Thread(new WrappedRunnable() {
               protected void runMayThrow() throws Exception {
                  SLF4JAuditWriter.logger.info("Flushing audit logger");

                  try {
                     Iterator appenders = logbackLogger.iteratorForAppenders();

                     while(appenders.hasNext()) {
                        Appender<ILoggingEvent> appender = (Appender)appenders.next();
                        appender.stop();
                     }
                  } catch (Exception var3) {
                     SLF4JAuditWriter.logger.warn("Error flushing audit logger, some messages may be dropped", var3);
                  }

               }
            });
            logFlusher.setName("Audit log flusher");
            Runtime.getRuntime().addShutdownHook(logFlusher);
         } else {
            String msg = "SLF4JAuditWriter logger is not an instance of ch.qos.logback.classic.Logger.\nNon-logback loggers are supported through slf4j, but they are not checked for additivity,\nand the are not explicitly closed on shutdown.";
            logger.warn(msg);
         }

      }
   }

   public Completable recordEvent(AuditableEvent event) {
      AUDIT_LOG.info("{}", event);
      return Completable.complete();
   }
}
