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
      if(error instanceof FSError || error instanceof CorruptSSTableException) {
         switch(null.$SwitchMap$org$apache$cassandra$config$Config$DiskFailurePolicy[DatabaseDescriptor.getDiskFailurePolicy().ordinal()]) {
         case 1:
         case 2:
         case 3:
            logger.error("Exiting forcefully due to file system exception on startup, disk failure policy \"{}\"", DatabaseDescriptor.getDiskFailurePolicy(), error);
            this.killer.killJVM(error, true);
         default:
         }
      }
   }
}
