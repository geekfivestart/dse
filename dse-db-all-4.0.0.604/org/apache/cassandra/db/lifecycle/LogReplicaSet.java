package org.apache.cassandra.db.lifecycle;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Map.Entry;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.utils.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogReplicaSet implements AutoCloseable {
   private static final Logger logger = LoggerFactory.getLogger(LogReplicaSet.class);
   private final Map<File, LogReplica> replicasByFile = new LinkedHashMap();

   public LogReplicaSet() {
   }

   private Collection<LogReplica> replicas() {
      return this.replicasByFile.values();
   }

   void addReplicas(List<File> replicas) {
      replicas.forEach(this::addReplica);
   }

   void addReplica(File file) {
      File directory = file.getParentFile();

      assert !this.replicasByFile.containsKey(directory);

      this.replicasByFile.put(directory, LogReplica.open(file));
      if(logger.isTraceEnabled()) {
         logger.trace("Added log file replica {} ", file);
      }

   }

   void maybeCreateReplica(File directory, String fileName, Set<LogRecord> records) {
      if(!this.replicasByFile.containsKey(directory)) {
         LogReplica replica = LogReplica.create(directory, fileName);
         records.forEach(replica::append);
         this.replicasByFile.put(directory, replica);
         if(logger.isTraceEnabled()) {
            logger.trace("Created new file replica {}", replica);
         }

      }
   }

   Throwable syncDirectory(Throwable accumulate) {
      return (Throwable)this.replicas().stream().map(LogReplica::syncDirectory).reduce(accumulate, (e1, e2) -> {
         return Throwables.merge(e1, e2);
      });
   }

   Throwable delete(Throwable accumulate) {
      return Throwables.perform(accumulate, this.replicas().stream().map((s) -> {
         return s::delete;
      }));
   }

   private static boolean isPrefixMatch(String first, String second) {
      return first.length() >= second.length()?first.startsWith(second):second.startsWith(first);
   }

   boolean readRecords(Set<LogRecord> records) {
      Map<LogReplica, List<String>> linesByReplica = (Map)this.replicas().stream().collect(Collectors.toMap(Function.identity(), LogReplica::readLines, (k, v) -> {
         throw new IllegalStateException("Duplicated key: " + k);
      }, LinkedHashMap::<init>));
      int maxNumLines = ((Integer)linesByReplica.values().stream().map(List::size).reduce(Integer.valueOf(0), Integer::max)).intValue();

      for(int i = 0; i < maxNumLines; ++i) {
         String firstLine = null;
         boolean partial = false;
         Iterator var7 = linesByReplica.entrySet().iterator();

         while(var7.hasNext()) {
            Entry<LogReplica, List<String>> entry = (Entry)var7.next();
            List<String> currentLines = (List)entry.getValue();
            if(i < currentLines.size()) {
               String currentLine = (String)currentLines.get(i);
               if(firstLine == null) {
                  firstLine = currentLine;
               } else {
                  if(!isPrefixMatch(firstLine, currentLine)) {
                     logger.error("Mismatched line in file {}: got '{}' expected '{}', giving up", new Object[]{((LogReplica)entry.getKey()).getFileName(), currentLine, firstLine});
                     ((LogReplica)entry.getKey()).setError(currentLine, String.format("Does not match <%s> in first replica file", new Object[]{firstLine}));
                     return false;
                  }

                  if(!firstLine.equals(currentLine)) {
                     if(i != currentLines.size() - 1) {
                        logger.error("Mismatched line in file {}: got '{}' expected '{}', giving up", new Object[]{((LogReplica)entry.getKey()).getFileName(), currentLine, firstLine});
                        ((LogReplica)entry.getKey()).setError(currentLine, String.format("Does not match <%s> in first replica file", new Object[]{firstLine}));
                        return false;
                     }

                     logger.warn("Mismatched last line in file {}: '{}' not the same as '{}'", new Object[]{((LogReplica)entry.getKey()).getFileName(), currentLine, firstLine});
                     if(currentLine.length() > firstLine.length()) {
                        firstLine = currentLine;
                     }

                     partial = true;
                  }
               }
            }
         }

         LogRecord record = LogRecord.make(firstLine);
         if(records.contains(record)) {
            logger.error("Found duplicate record {} for {}, giving up", record, record.fileName());
            this.setError(record, "Duplicated record");
            return false;
         }

         if(partial) {
            record.setPartial();
         }

         records.add(record);
         if(record.isFinal() && i != maxNumLines - 1) {
            logger.error("Found too many lines for {}, giving up", record.fileName());
            this.setError(record, "This record should have been the last one in all file replicas");
            return false;
         }
      }

      return true;
   }

   void setError(LogRecord record, String error) {
      record.setError(error);
      this.setErrorInReplicas(record);
   }

   void setErrorInReplicas(LogRecord record) {
      this.replicas().forEach((r) -> {
         r.setError(record.raw, record.error());
      });
   }

   void printContentsWithAnyErrors(StringBuilder str) {
      this.replicas().forEach((r) -> {
         r.printContentsWithAnyErrors(str);
      });
   }

   void append(LogRecord record) {
      Throwable err = null;
      int failed = 0;
      Iterator var4 = this.replicas().iterator();

      while(var4.hasNext()) {
         LogReplica replica = (LogReplica)var4.next();

         try {
            replica.append(record);
         } catch (Throwable var7) {
            logger.warn("Failed to add record to a replica: {}", var7.getMessage());
            err = Throwables.merge(err, var7);
            ++failed;
         }
      }

      if(err != null) {
         if(!record.isFinal() || failed == this.replicas().size()) {
            Throwables.maybeFail(err);
         }

         logger.error("Failed to add record '{}' to some replicas '{}'", new Object[]{record, this, err});
      }

   }

   boolean exists() {
      Optional<Boolean> ret = this.replicas().stream().map(LogReplica::exists).reduce(Boolean::logicalAnd);
      return ret.isPresent()?((Boolean)ret.get()).booleanValue():false;
   }

   public void close() {
      Throwables.maybeFail(Throwables.perform((Throwable)null, (Stream)this.replicas().stream().map((r) -> {
         return r::close;
      })));
   }

   public String toString() {
      Optional<String> ret = this.replicas().stream().map(LogReplica::toString).reduce(String::concat);
      return ret.isPresent()?(String)ret.get():"[-]";
   }

   String getDirectories() {
      return String.join(", ", (Iterable)this.replicas().stream().map(LogReplica::getDirectory).collect(Collectors.toList()));
   }

   @VisibleForTesting
   List<File> getFiles() {
      return (List)this.replicas().stream().map(LogReplica::file).collect(Collectors.toList());
   }

   @VisibleForTesting
   List<String> getFilePaths() {
      return (List)this.replicas().stream().map(LogReplica::file).map(File::getPath).collect(Collectors.toList());
   }
}
