package org.apache.cassandra.net;

import java.util.concurrent.TimeUnit;
import org.apache.cassandra.exceptions.InternalRequestExecutionException;
import org.apache.cassandra.utils.NoSpamLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ErrorHandler {
   Logger logger = LoggerFactory.getLogger(VerbHandlers.class);
   NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 5L, TimeUnit.MINUTES);
   ErrorHandler DEFAULT = (error) -> {
      noSpamLogger.warn(error.getMessage(), new Object[0]);
   };

   void handleError(InternalRequestExecutionException var1);
}
