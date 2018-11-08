package org.apache.cassandra.db.lifecycle;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Throwables;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class LogFile implements AutoCloseable {
   private static final Logger logger = LoggerFactory.getLogger(LogFile.class);
   static String EXT = ".log";
   static char SEP = 95;
   static Pattern FILE_REGEX;
   private final LogReplicaSet replicas;
   private final LinkedHashSet<LogRecord> records;
   private final OperationType type;
   private final UUID id;

   static LogFile make(File logReplica) {
      return make(logReplica.getName(), Collections.singletonList(logReplica));
   }

   static LogFile make(String fileName, List<File> logReplicas) {
      Matcher matcher = FILE_REGEX.matcher(fileName);
      boolean matched = matcher.matches();

      assert matched && matcher.groupCount() == 3;

      OperationType operationType = OperationType.fromFileName(matcher.group(2));
      UUID id = UUID.fromString(matcher.group(3));
      return new LogFile(operationType, id, logReplicas);
   }

   Throwable syncDirectory(Throwable accumulate) {
      return this.replicas.syncDirectory(accumulate);
   }

   OperationType type() {
      return this.type;
   }

   UUID id() {
      return this.id;
   }

   Throwable removeUnfinishedLeftovers(Throwable accumulate) {
      try {
         Throwables.maybeFail(this.syncDirectory(accumulate));
         this.deleteFilesForRecordsOfType(this.committed()?LogRecord.Type.REMOVE:LogRecord.Type.ADD);
         Throwables.maybeFail(this.syncDirectory(accumulate));
         accumulate = this.replicas.delete(accumulate);
      } catch (Throwable var3) {
         accumulate = Throwables.merge(accumulate, var3);
      }

      return accumulate;
   }

   static boolean isLogFile(File file) {
      return FILE_REGEX.matcher(file.getName()).matches();
   }

   LogFile(OperationType type, UUID id, List<File> replicas) {
      this(type, id);
      this.replicas.addReplicas(replicas);
   }

   LogFile(OperationType type, UUID id) {
      this.replicas = new LogReplicaSet();
      this.records = new LinkedHashSet();
      this.type = type;
      this.id = id;
   }

   boolean verify() {
      this.records.clear();
      if(!this.replicas.readRecords(this.records)) {
         logger.error("Failed to read records for transaction log {}", this);
         return false;
      } else {
         this.records.forEach(LogFile::verifyRecord);
         Optional<LogRecord> firstInvalid = this.records.stream().filter(LogRecord::isInvalidOrPartial).findFirst();
         if(!firstInvalid.isPresent()) {
            return true;
         } else {
            LogRecord failedOn = (LogRecord)firstInvalid.get();
            if(this.getLastRecord() != failedOn) {
               this.setErrorInReplicas(failedOn);
               return false;
            } else {
               this.records.stream().filter((r) -> {
                  return r != failedOn;
               }).forEach(LogFile::verifyRecordWithCorruptedLastRecord);
               if(this.records.stream().filter((r) -> {
                  return r != failedOn;
               }).filter(LogRecord::isInvalid).map(this::setErrorInReplicas).findFirst().isPresent()) {
                  this.setErrorInReplicas(failedOn);
                  return false;
               } else {
                  logger.warn("Last record of transaction {} is corrupt or incomplete [{}], but all previous records match state on disk; continuing", this.id, failedOn.error());
                  return true;
               }
            }
         }
      }
   }

   LogRecord setErrorInReplicas(LogRecord record) {
      this.replicas.setErrorInReplicas(record);
      return record;
   }

   static void verifyRecord(LogRecord record) {
      if(record.checksum != record.computeChecksum()) {
         record.setError(String.format("Invalid checksum for sstable [%s]: [%d] should have been [%d]", new Object[]{record.fileName(), Long.valueOf(record.checksum), Long.valueOf(record.computeChecksum())}));
      } else if(record.type == LogRecord.Type.REMOVE) {
         record.status.onDiskRecord = record.withExistingFiles();
         if(record.updateTime != record.status.onDiskRecord.updateTime && record.status.onDiskRecord.updateTime > 0L) {
            record.setError(String.format("Unexpected files detected for sstable [%s]: last update time [%tT] should have been [%tT]", new Object[]{record.fileName(), Long.valueOf(record.status.onDiskRecord.updateTime), Long.valueOf(record.updateTime)}));
         }

      }
   }

   static void verifyRecordWithCorruptedLastRecord(LogRecord record) {
      if(record.type == LogRecord.Type.REMOVE && record.status.onDiskRecord.numFiles < record.numFiles) {
         record.setError(String.format("Incomplete fileset detected for sstable [%s]: number of files [%d] should have been [%d].", new Object[]{record.fileName(), Integer.valueOf(record.status.onDiskRecord.numFiles), Integer.valueOf(record.numFiles)}));
      }

   }

   void commit() {
      if(this.completed()) {
         throw new IllegalStateException("Already completed");
      } else {
         this.addRecord(LogRecord.makeCommit(System.currentTimeMillis()));
      }
   }

   void abort() {
      if(this.completed()) {
         throw new IllegalStateException("Already completed");
      } else {
         this.addRecord(LogRecord.makeAbort(System.currentTimeMillis()));
      }
   }

   private boolean isLastRecordValidWithType(LogRecord.Type type) {
      LogRecord lastRecord = this.getLastRecord();
      return lastRecord != null && lastRecord.type == type && lastRecord.isValid();
   }

   boolean committed() {
      return this.isLastRecordValidWithType(LogRecord.Type.COMMIT);
   }

   boolean aborted() {
      return this.isLastRecordValidWithType(LogRecord.Type.ABORT);
   }

   boolean completed() {
      return this.committed() || this.aborted();
   }

   void add(LogRecord.Type type, SSTable table) {
      this.add(this.makeRecord(type, table));
   }

   void add(LogRecord record) {
      if(!this.addRecord(record)) {
         throw new IllegalStateException();
      }
   }

   public void addAll(LogRecord.Type type, Iterable<SSTableReader> toBulkAdd) {
      Iterator var3 = this.makeRecords(type, toBulkAdd).values().iterator();

      LogRecord record;
      do {
         if(!var3.hasNext()) {
            return;
         }

         record = (LogRecord)var3.next();
      } while(this.addRecord(record));

      throw new IllegalStateException("Record already contained in txn log");
   }

   Map<SSTable, LogRecord> makeRecords(LogRecord.Type type, Iterable<SSTableReader> tables) {
      assert type == LogRecord.Type.ADD || type == LogRecord.Type.REMOVE;

      Iterator var3 = tables.iterator();

      while(var3.hasNext()) {
         SSTableReader sstable = (SSTableReader)var3.next();
         File directory = sstable.descriptor.directory;
         String fileName = StringUtils.join(new Serializable[]{directory, File.separator, this.getFileName()});
         this.replicas.maybeCreateReplica(directory, fileName, this.records);
      }

      return LogRecord.make(type, tables);
   }

   private LogRecord makeRecord(LogRecord.Type type, SSTable table) {
      assert type == LogRecord.Type.ADD || type == LogRecord.Type.REMOVE;

      File directory = table.descriptor.directory;
      String fileName = StringUtils.join(new Serializable[]{directory, File.separator, this.getFileName()});
      this.replicas.maybeCreateReplica(directory, fileName, this.records);
      return LogRecord.make(type, table);
   }

   private LogRecord makeRecord(LogRecord.Type type, SSTable table, LogRecord record) {
      assert type == LogRecord.Type.ADD || type == LogRecord.Type.REMOVE;

      File directory = table.descriptor.directory;
      String fileName = StringUtils.join(new Serializable[]{directory, File.separator, this.getFileName()});
      this.replicas.maybeCreateReplica(directory, fileName, this.records);
      return record.asType(type);
   }

   private boolean addRecord(LogRecord record) {
      if(this.records.contains(record)) {
         return false;
      } else {
         this.replicas.append(record);
         return this.records.add(record);
      }
   }

   void remove(LogRecord.Type type, SSTable table) {
      LogRecord record = this.makeRecord(type, table);

      assert this.records.contains(record) : String.format("[%s] is not tracked by %s", new Object[]{record, this.id});

      deleteRecordFiles(record);
      this.records.remove(record);
   }

   boolean contains(LogRecord.Type type, SSTable table) {
      return this.contains(this.makeRecord(type, table));
   }

   boolean contains(LogRecord.Type type, SSTable sstable, LogRecord record) {
      return this.contains(this.makeRecord(type, sstable, record));
   }

   private boolean contains(LogRecord record) {
      return this.records.contains(record);
   }

   void deleteFilesForRecordsOfType(LogRecord.Type type) {
      Stream var10000 = this.records.stream();
      type.getClass();
      var10000.filter(type::matches).forEach(LogFile::deleteRecordFiles);
      this.records.clear();
   }

   private static void deleteRecordFiles(LogRecord record) {
      List<File> files = record.getExistingFiles();
      files.sort((f1, f2) -> {
         return Long.compare(f1.lastModified(), f2.lastModified());
      });
      files.forEach(LogTransaction::delete);
   }

   Map<LogRecord, Set<File>> getFilesOfType(Path folder, NavigableSet<File> files, LogRecord.Type type) {
      Map<LogRecord, Set<File>> ret = new HashMap();
      Stream var10000 = this.records.stream();
      type.getClass();
      var10000.filter(type::matches).filter(LogRecord::isValid).filter((r) -> {
         return r.isInFolder(folder);
      }).forEach((r) -> {
         Set var10000 = (Set)ret.put(r, getRecordFiles(files, r));
      });
      return ret;
   }

   LogRecord getLastRecord() {
      return (LogRecord)Iterables.getLast(this.records, (Object)null);
   }

   private static Set<File> getRecordFiles(NavigableSet<File> files, LogRecord record) {
      String fileName = record.fileName();
      return (Set)files.stream().filter((f) -> {
         return f.getName().startsWith(fileName);
      }).collect(Collectors.toSet());
   }

   boolean exists() {
      return this.replicas.exists();
   }

   public void close() {
      this.replicas.close();
   }

   public String toString() {
      return this.toString(false);
   }

   public String toString(boolean showContents) {
      StringBuilder str = new StringBuilder();
      str.append('[');
      str.append(this.getFileName());
      str.append(" in ");
      str.append(this.replicas.getDirectories());
      str.append(']');
      if(showContents) {
         str.append(System.lineSeparator());
         str.append("Files and contents follow:");
         str.append(System.lineSeparator());
         this.replicas.printContentsWithAnyErrors(str);
      }

      return str.toString();
   }

   @VisibleForTesting
   List<File> getFiles() {
      return this.replicas.getFiles();
   }

   @VisibleForTesting
   List<String> getFilePaths() {
      return this.replicas.getFilePaths();
   }

   private String getFileName() {
      return StringUtils.join(new Object[]{SSTableFormat.current().getLatestVersion(), Character.valueOf(SEP), "txn", Character.valueOf(SEP), this.type.fileName, Character.valueOf(SEP), this.id.toString(), EXT});
   }

   public boolean isEmpty() {
      return this.records.isEmpty();
   }

   static {
      FILE_REGEX = Pattern.compile(String.format("^(.{2})_txn_(.*)_(.*)%s$", new Object[]{EXT}));
   }
}
