package org.apache.cassandra.db.lifecycle;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.cassandra.db.Directories;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class LogAwareFileLister {
   private static final Logger logger = LoggerFactory.getLogger(LogAwareFileLister.class);
   private final Path folder;
   private final BiPredicate<File, Directories.FileType> filter;
   private final Directories.OnTxnErr onTxnErr;
   NavigableMap<File, Directories.FileType> files = new TreeMap();

   @VisibleForTesting
   LogAwareFileLister(Path folder, BiPredicate<File, Directories.FileType> filter, Directories.OnTxnErr onTxnErr) {
      this.folder = folder;
      this.filter = filter;
      this.onTxnErr = onTxnErr;
   }

   public List<File> list() {
      try {
         return this.innerList();
      } catch (Throwable var2) {
         throw new RuntimeException(String.format("Failed to list files in %s", new Object[]{this.folder}), var2);
      }
   }

   List<File> innerList() throws Throwable {
      list(Files.newDirectoryStream(this.folder)).stream().filter((f) -> {
         return !LogFile.isLogFile(f);
      }).forEach((f) -> {
         Directories.FileType var10000 = (Directories.FileType)this.files.put(f, Directories.FileType.FINAL);
      });
      list(Files.newDirectoryStream(this.folder, '*' + LogFile.EXT)).stream().filter(LogFile::isLogFile).forEach(this::classifyFiles);
      return (List)this.files.entrySet().stream().filter((e) -> {
         return this.filter.test(e.getKey(), e.getValue());
      }).map(Entry::getKey).collect(Collectors.toList());
   }

   static List<File> list(DirectoryStream<Path> stream) throws IOException {
      List var1;
      try {
         var1 = (List)StreamSupport.stream(stream.spliterator(), false).map(Path::toFile).filter((f) -> {
            return !f.isDirectory();
         }).collect(Collectors.toList());
      } finally {
         stream.close();
      }

      return var1;
   }

   void classifyFiles(File txnFile) {
      LogFile txn = LogFile.make(txnFile);
      Throwable var3 = null;

      try {
         this.readTxnLog(txn);
         this.classifyFiles(txn);
         this.files.put(txnFile, Directories.FileType.TXN_LOG);
      } catch (Throwable var12) {
         var3 = var12;
         throw var12;
      } finally {
         if(txn != null) {
            if(var3 != null) {
               try {
                  txn.close();
               } catch (Throwable var11) {
                  var3.addSuppressed(var11);
               }
            } else {
               txn.close();
            }
         }

      }

   }

   void readTxnLog(LogFile txn) {
      if(!txn.verify() && this.onTxnErr == Directories.OnTxnErr.THROW) {
         throw new LogTransaction.CorruptTransactionLogException("Some records failed verification. See earlier in log for details.", txn);
      }
   }

   void classifyFiles(LogFile txnFile) {
      Map<LogRecord, Set<File>> oldFiles = txnFile.getFilesOfType(this.folder, this.files.navigableKeySet(), LogRecord.Type.REMOVE);
      Map<LogRecord, Set<File>> newFiles = txnFile.getFilesOfType(this.folder, this.files.navigableKeySet(), LogRecord.Type.ADD);
      if(txnFile.completed()) {
         this.setTemporary(txnFile, oldFiles.values(), newFiles.values());
      } else if(allFilesPresent(oldFiles)) {
         this.setTemporary(txnFile, oldFiles.values(), newFiles.values());
      } else if(txnFile.exists()) {
         this.readTxnLog(txnFile);
         if(txnFile.completed()) {
            this.setTemporary(txnFile, oldFiles.values(), newFiles.values());
         } else {
            logger.error("Failed to classify files in {}\nSome old files are missing but the txn log is still there and not completed\nFiles in folder:\n{}\nTxn: {}", new Object[]{this.folder, this.files.isEmpty()?"\t-":String.join("\n", (Iterable)this.files.keySet().stream().map((f) -> {
               return String.format("\t%s", new Object[]{f});
            }).collect(Collectors.toList())), txnFile.toString(true)});
            throw new RuntimeException(String.format("Failed to list directory files in %s, inconsistent disk state for transaction %s", new Object[]{this.folder, txnFile}));
         }
      }
   }

   private static boolean allFilesPresent(Map<LogRecord, Set<File>> oldFiles) {
      return !oldFiles.entrySet().stream().filter((e) -> {
         return ((LogRecord)e.getKey()).numFiles > ((Set)e.getValue()).size();
      }).findFirst().isPresent();
   }

   private void setTemporary(LogFile txnFile, Collection<Set<File>> oldFiles, Collection<Set<File>> newFiles) {
      Collection<Set<File>> temporary = txnFile.committed()?oldFiles:newFiles;
      temporary.stream().flatMap(Collection::stream).forEach((f) -> {
         Directories.FileType var10000 = (Directories.FileType)this.files.put(f, Directories.FileType.TEMPORARY);
      });
   }
}
