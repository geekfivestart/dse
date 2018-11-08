package com.datastax.bdp.cassandra.audit;

import com.datastax.bdp.db.audit.AuditableEvent;
import com.datastax.bdp.db.audit.IAuditLogger;
import java.util.List;
import org.apache.cassandra.concurrent.TPCUtils;

public class AuditLoggerUtils {
   public AuditLoggerUtils() {
   }

   public static void logEventBlocking(IAuditLogger logger, AuditableEvent event) {
      TPCUtils.blockingAwait(logger.logEvent(event));
   }

   public static void logEventsBlocking(IAuditLogger logger, List<AuditableEvent> events) {
      TPCUtils.blockingAwait(logger.logEvents(events));
   }
}
