package org.apache.cassandra.utils;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class StartupDiskErrorHandler implements ErrorHandler {
   private static final Logger logger = LoggerFactory.getLogger(StartupDiskErrorHandler.class);
   private final JVMKiller killer;

   StartupDiskErrorHandler(JVMKiller killer) {
      this.killer = killer;
   }

   public void handleError(Throwable error) {
      if (!(error instanceof FSError) && !(error instanceof CorruptSSTableException)) {
         return;
      }
      switch (DatabaseDescriptor.getDiskFailurePolicy()) {
         case stop_paranoid:
         case stop:
         case die: {
            logger.error("Exiting forcefully due to file system exception on startup, disk failure policy \"{}\"", (Object)DatabaseDescriptor.getDiskFailurePolicy(), (Object)error);
            this.killer.killJVM(error, true);
            break;
         }
      }
   }
}
