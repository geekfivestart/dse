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
      switch (DatabaseDescriptor.getDiskFailurePolicy()) {
         case stop_paranoid: {
            this.storageService.stopTransportsAsync();
            break;
         }
         case die: {
            this.killer.killJVM(e, false);
         }
      }
   }

   private void handleFSError(FSError e) {
      switch (DatabaseDescriptor.getDiskFailurePolicy()) {
         case stop_paranoid:
         case stop: {
            this.storageService.stopTransportsAsync();
            break;
         }
         case best_effort: {
            File directory;
            if (!e.path.isPresent()) break;
            BlacklistedDirectories.maybeMarkUnwritable(e.path.get());
            if (!(e instanceof FSReadError) || (directory = BlacklistedDirectories.maybeMarkUnreadable(e.path.get())) == null) break;
            Keyspace.removeUnreadableSSTables(directory);
            break;
         }
         case ignore: {
            logger.error("Ignoring file system error {}/{} as per ignore disk failure policy", e.getClass(), (Object)e.getMessage());
            break;
         }
         case die: {
            this.killer.killJVM(e, false);
            break;
         }
         default: {
            throw new IllegalStateException();
         }
      }
   }
}
