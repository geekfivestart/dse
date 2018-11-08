package org.apache.cassandra.utils;

import java.io.File;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BlacklistedDirectories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DefaultDiskErrorHandler implements ErrorHandler {
   private static final Logger logger = LoggerFactory.getLogger(DefaultDiskErrorHandler.class);
   private final JVMKiller killer;
   private final StorageService storageService;

   public DefaultDiskErrorHandler(JVMKiller killer, StorageService storageService) {
      this.killer = killer;
      this.storageService = storageService;
   }

   public void handleError(Throwable error) {
      if(error instanceof FSError) {
         this.handleFSError((FSError)error);
      } else if(error instanceof CorruptSSTableException) {
         this.handleCorruptSSTable((CorruptSSTableException)error);
      }

   }

   private void handleCorruptSSTable(CorruptSSTableException e) {
      switch(null.$SwitchMap$org$apache$cassandra$config$Config$DiskFailurePolicy[DatabaseDescriptor.getDiskFailurePolicy().ordinal()]) {
      case 1:
         this.storageService.stopTransportsAsync();
         break;
      case 2:
         this.killer.killJVM(e, false);
      }

   }

   private void handleFSError(FSError e) {
      switch(null.$SwitchMap$org$apache$cassandra$config$Config$DiskFailurePolicy[DatabaseDescriptor.getDiskFailurePolicy().ordinal()]) {
      case 1:
      case 3:
         this.storageService.stopTransportsAsync();
         break;
      case 2:
         this.killer.killJVM(e, false);
         break;
      case 4:
         if(e.path.isPresent()) {
            BlacklistedDirectories.maybeMarkUnwritable((File)e.path.get());
            if(e instanceof FSReadError) {
               File directory = BlacklistedDirectories.maybeMarkUnreadable((File)e.path.get());
               if(directory != null) {
                  Keyspace.removeUnreadableSSTables(directory);
               }
            }
         }
         break;
      case 5:
         logger.error("Ignoring file system error {}/{} as per ignore disk failure policy", e.getClass(), e.getMessage());
         break;
      default:
         throw new IllegalStateException();
      }

   }
}
