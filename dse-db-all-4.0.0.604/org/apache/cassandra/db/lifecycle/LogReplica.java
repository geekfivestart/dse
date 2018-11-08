package org.apache.cassandra.db.lifecycle;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.NativeLibrary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class LogReplica implements AutoCloseable {
   private static final Logger logger = LoggerFactory.getLogger(LogReplica.class);
   private final File file;
   private int directoryDescriptor;
   private final Map<String, String> errors = new HashMap();

   static LogReplica create(File directory, String fileName) {
      return new LogReplica(new File(fileName), NativeLibrary.tryOpenDirectory(directory.getPath()));
   }

   static LogReplica open(File file) {
      return new LogReplica(file, NativeLibrary.tryOpenDirectory(file.getParentFile().getPath()));
   }

   LogReplica(File file, int directoryDescriptor) {
      this.file = file;
      this.directoryDescriptor = directoryDescriptor;
   }

   File file() {
      return this.file;
   }

   List<String> readLines() {
      return FileUtils.readLines(this.file);
   }

   String getFileName() {
      return this.file.getName();
   }

   String getDirectory() {
      return this.file.getParent();
   }

   void append(LogRecord record) {
      boolean existed = this.exists();
      FileUtils.appendAndSync(this.file, new String[]{record.toString()});
      if(!existed) {
         this.syncDirectory();
      }

   }

   Throwable syncDirectory() {
      try {
         if(this.directoryDescriptor >= 0) {
            NativeLibrary.trySync(this.directoryDescriptor);
         }

         return null;
      } catch (Throwable var2) {
         logger.warn("Failed to sync directory: {}", var2.getMessage());
         return var2;
      }
   }

   void delete() {
      LogTransaction.delete(this.file);
      this.syncDirectory();
   }

   boolean exists() {
      return this.file.exists();
   }

   public void close() {
      if(this.directoryDescriptor >= 0) {
         NativeLibrary.tryCloseFD(this.directoryDescriptor);
         this.directoryDescriptor = -1;
      }

   }

   public String toString() {
      return String.format("[%s] ", new Object[]{this.file});
   }

   void setError(String line, String error) {
      this.errors.put(line, error);
   }

   void printContentsWithAnyErrors(StringBuilder str) {
      str.append(this.file.getPath());
      str.append(System.lineSeparator());
      FileUtils.readLines(this.file).forEach((line) -> {
         this.printLineWithAnyError(str, line);
      });
   }

   private void printLineWithAnyError(StringBuilder str, String line) {
      str.append('\t');
      str.append(line);
      str.append(System.lineSeparator());
      String error = (String)this.errors.get(line);
      if(error != null) {
         str.append("\t\t***");
         str.append(error);
         str.append(System.lineSeparator());
      }

   }
}
