package org.apache.cassandra.db.lifecycle;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Runnables;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SnapshotDeletingTask;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LogTransaction extends Transactional.AbstractTransactional implements Transactional {
   private static final Logger logger = LoggerFactory.getLogger(LogTransaction.class);
   private final Tracker tracker;
   private final LogFile txnFile;
   private final Ref<LogTransaction> selfRef;
   private static final Queue<Runnable> failedDeletions = new ConcurrentLinkedQueue();

   LogTransaction(OperationType opType) {
      this(opType, (Tracker)null);
   }

   LogTransaction(OperationType opType, Tracker tracker) {
      this.tracker = tracker;
      this.txnFile = new LogFile(opType, UUIDGen.getTimeUUID());
      this.selfRef = new Ref(this, new LogTransaction.TransactionTidier(this.txnFile));
      if(logger.isTraceEnabled()) {
         logger.trace("Created transaction logs with id {}", this.txnFile.id());
      }

   }

   void trackNew(SSTable table) {
      this.txnFile.add(LogRecord.Type.ADD, table);
   }

   void untrackNew(SSTable table) {
      this.txnFile.remove(LogRecord.Type.ADD, table);
   }

   @VisibleForTesting
   LogTransaction.SSTableTidier obsoleted(SSTableReader sstable) {
      return this.obsoleted(sstable, LogRecord.make(LogRecord.Type.REMOVE, (SSTable)sstable));
   }

   LogTransaction.SSTableTidier obsoleted(SSTableReader reader, LogRecord logRecord) {
      if(this.txnFile.contains(LogRecord.Type.ADD, reader, logRecord)) {
         if(this.txnFile.contains(LogRecord.Type.REMOVE, reader, logRecord)) {
            throw new IllegalArgumentException();
         } else {
            return new LogTransaction.SSTableTidier(reader, true, this);
         }
      } else {
         this.txnFile.add(logRecord);
         if(this.tracker != null) {
            this.tracker.notifyDeleting(reader);
         }

         return new LogTransaction.SSTableTidier(reader, false, this);
      }
   }

   Map<SSTable, LogRecord> makeRemoveRecords(Iterable<SSTableReader> sstables) {
      return this.txnFile.makeRecords(LogRecord.Type.REMOVE, sstables);
   }

   OperationType type() {
      return this.txnFile.type();
   }

   UUID id() {
      return this.txnFile.id();
   }

   @VisibleForTesting
   LogFile txnFile() {
      return this.txnFile;
   }

   @VisibleForTesting
   List<File> logFiles() {
      return this.txnFile.getFiles();
   }

   @VisibleForTesting
   List<String> logFilePaths() {
      return this.txnFile.getFilePaths();
   }

   static void delete(File file) {
      try {
         if(logger.isTraceEnabled()) {
            logger.trace("Deleting {}", file);
         }

         Files.delete(file.toPath());
      } catch (NoSuchFileException var17) {
         NoSuchFileException e = var17;
         logger.error("Unable to delete {} as it does not exist, see debug log file for stack trace", file);
         if(logger.isDebugEnabled()) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);
            Throwable var4 = null;

            try {
               e.printStackTrace(ps);
            } catch (Throwable var15) {
               var4 = var15;
               throw var15;
            } finally {
               if(ps != null) {
                  if(var4 != null) {
                     try {
                        ps.close();
                     } catch (Throwable var14) {
                        var4.addSuppressed(var14);
                     }
                  } else {
                     ps.close();
                  }
               }

            }

            logger.debug("Unable to delete {} as it does not exist, stack trace:\n {}", file, baos);
         }
      } catch (IOException var18) {
         logger.error("Unable to delete {}", file, var18);
         throw new RuntimeException(var18);
      }

   }

   static void rescheduleFailedDeletions() {
      Runnable task;
      while(null != (task = (Runnable)failedDeletions.poll())) {
         ScheduledExecutors.nonPeriodicTasks.submit(task);
      }

      SnapshotDeletingTask.rescheduleFailedTasks();
   }

   static void waitForDeletions() {
      FBUtilities.waitOnFuture(ScheduledExecutors.nonPeriodicTasks.schedule(Runnables.doNothing(), 0L, TimeUnit.MILLISECONDS));
   }

   @VisibleForTesting
   Throwable complete(Throwable accumulate) {
      try {
         accumulate = this.selfRef.ensureReleased(accumulate);
         return accumulate;
      } catch (Throwable var3) {
         logger.error("Failed to complete file transaction id {}", this.id(), var3);
         return Throwables.merge(accumulate, var3);
      }
   }

   protected Throwable doCommit(Throwable accumulate) {
      LogFile var10002 = this.txnFile;
      this.txnFile.getClass();
      return this.complete(Throwables.perform(accumulate, var10002::commit));
   }

   protected Throwable doAbort(Throwable accumulate) {
      LogFile var10002 = this.txnFile;
      this.txnFile.getClass();
      return this.complete(Throwables.perform(accumulate, var10002::abort));
   }

   protected void doPrepare() {
   }

   static boolean removeUnfinishedLeftovers(TableMetadata metadata) {
      return removeUnfinishedLeftovers((new Directories(metadata, ColumnFamilyStore.getInitialDirectories())).getCFDirectories());
   }

   @VisibleForTesting
   static boolean removeUnfinishedLeftovers(List<File> directories) {
      LogTransaction.LogFilesByName logFiles = new LogTransaction.LogFilesByName();
      directories.forEach(logFiles::list);
      return logFiles.removeUnfinishedLeftovers();
   }

   private static final class LogFilesByName {
      Map<String, List<File>> files;

      private LogFilesByName() {
         this.files = new HashMap();
      }

      void list(File directory) {
         Arrays.stream(directory.listFiles(LogFile::isLogFile)).forEach(this::add);
      }

      void add(File file) {
         List<File> filesByName = (List)this.files.get(file.getName());
         if(filesByName == null) {
            filesByName = new ArrayList();
            this.files.put(file.getName(), filesByName);
         }

         ((List)filesByName).add(file);
      }

      boolean removeUnfinishedLeftovers() {
         return this.files.entrySet().stream().map(LogTransaction.LogFilesByName::removeUnfinishedLeftovers).allMatch(Predicate.isEqual(Boolean.valueOf(true)));
      }

      static boolean removeUnfinishedLeftovers(Entry<String, List<File>> entry) {
         LogFile txn = LogFile.make((String)entry.getKey(), (List)entry.getValue());
         Throwable var2 = null;

         boolean var3;
         try {
            if(txn.verify()) {
               Throwable failure = txn.removeUnfinishedLeftovers((Throwable)null);
               boolean var4;
               if(failure != null) {
                  LogTransaction.logger.error("Failed to remove unfinished transaction leftovers for transaction log {}", txn.toString(true), failure);
                  var4 = false;
                  return var4;
               }

               var4 = true;
               return var4;
            }

            LogTransaction.logger.error("Unexpected disk state: failed to read transaction log {}\nCheck logs before last shutdown for any errors, and ensure txn log files were not edited manually.", txn.toString(true));
            var3 = false;
         } catch (Throwable var15) {
            var2 = var15;
            throw var15;
         } finally {
            if(txn != null) {
               if(var2 != null) {
                  try {
                     txn.close();
                  } catch (Throwable var14) {
                     var2.addSuppressed(var14);
                  }
               } else {
                  txn.close();
               }
            }

         }

         return var3;
      }
   }

   public static class SSTableTidier implements Runnable {
      private final Descriptor desc;
      private final long sizeOnDisk;
      private final Tracker tracker;
      private final boolean wasNew;
      private final Ref<LogTransaction> parentRef;

      public SSTableTidier(SSTableReader referent, boolean wasNew, LogTransaction parent) {
         this.desc = referent.descriptor;
         this.sizeOnDisk = referent.bytesOnDisk();
         this.tracker = parent.tracker;
         this.wasNew = wasNew;
         this.parentRef = parent.selfRef.tryRef();
      }

      public void run() {
         if(this.tracker != null && !this.tracker.isDummy()) {
            TPCUtils.blockingAwait(SystemKeyspace.clearSSTableReadMeter(this.desc.ksname, this.desc.cfname, this.desc.generation));
         }

         try {
            File datafile = new File(this.desc.filenameFor(Component.DATA));
            if(datafile.exists()) {
               LogTransaction.delete(datafile);
            } else if(!this.wasNew) {
               LogTransaction.logger.error("SSTableTidier ran with no existing data file for an sstable that was not new");
            }

            SSTable.delete(this.desc, SSTable.discoverComponentsFor(this.desc));
         } catch (Throwable var2) {
            LogTransaction.logger.error("Failed deletion for {}, we'll retry after GC and on server restart", this.desc);
            LogTransaction.failedDeletions.add(this);
            return;
         }

         if(this.tracker != null && this.tracker.cfstore != null && !this.wasNew) {
            this.tracker.cfstore.metric.totalDiskSpaceUsed.dec(this.sizeOnDisk);
         }

         this.parentRef.release();
      }

      public void abort() {
         this.parentRef.release();
      }
   }

   static class Obsoletion {
      final SSTableReader reader;
      final LogTransaction.SSTableTidier tidier;

      Obsoletion(SSTableReader reader, LogTransaction.SSTableTidier tidier) {
         this.reader = reader;
         this.tidier = tidier;
      }
   }

   private static class TransactionTidier implements RefCounted.Tidy, Runnable {
      private final LogFile data;

      TransactionTidier(LogFile data) {
         this.data = data;
      }

      public void tidy() throws Exception {
         this.run();
      }

      public String name() {
         return this.data.toString();
      }

      public void run() {
         if(LogTransaction.logger.isTraceEnabled()) {
            LogTransaction.logger.trace("Removing files for transaction log {}", this.data);
         }

         Throwable err;
         if(!this.data.completed()) {
            LogTransaction.logger.error("Transaction log {} indicates txn was not completed, trying to abort it now", this.data);
            Throwable var10000 = (Throwable)null;
            LogFile var10001 = this.data;
            this.data.getClass();
            err = Throwables.perform(var10000, var10001::abort);
            if(err != null) {
               LogTransaction.logger.error("Failed to abort transaction log {}", this.data, err);
            }
         }

         err = this.data.removeUnfinishedLeftovers((Throwable)null);
         if(err != null) {
            LogTransaction.logger.info("Failed deleting files for transaction log {}, we'll retry after GC and on on server restart", this.data, err);
            LogTransaction.failedDeletions.add(this);
         } else {
            if(LogTransaction.logger.isTraceEnabled()) {
               LogTransaction.logger.trace("Closing transaction log {}", this.data);
            }

            this.data.close();
         }

      }
   }

   public static final class CorruptTransactionLogException extends RuntimeException {
      public final LogFile txnFile;

      public CorruptTransactionLogException(String message, LogFile txnFile) {
         super(message);
         this.txnFile = txnFile;
      }
   }
}
