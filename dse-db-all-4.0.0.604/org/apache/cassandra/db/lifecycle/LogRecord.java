package org.apache.cassandra.db.lifecycle;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Map.Entry;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;

final class LogRecord {
   public final LogRecord.Type type;
   public final Optional<String> absolutePath;
   public final long updateTime;
   public final int numFiles;
   public final String raw;
   public final long checksum;
   public final LogRecord.Status status;
   static Pattern REGEX = Pattern.compile("^(add|remove|commit|abort):\\[([^,]*),?([^,]*),?([^,]*)\\]\\[(\\d*)\\]$", 2);

   public static LogRecord make(String line) {
      try {
         Matcher matcher = REGEX.matcher(line);
         if(!matcher.matches()) {
            return (new LogRecord(LogRecord.Type.UNKNOWN, (String)null, 0L, 0, 0L, line)).setError(String.format("Failed to parse [%s]", new Object[]{line}));
         } else {
            LogRecord.Type type = LogRecord.Type.fromPrefix(matcher.group(1));
            return new LogRecord(type, matcher.group(2), Long.parseLong(matcher.group(3)), Integer.parseInt(matcher.group(4)), Long.parseLong(matcher.group(5)), line);
         }
      } catch (IllegalArgumentException var3) {
         return (new LogRecord(LogRecord.Type.UNKNOWN, (String)null, 0L, 0, 0L, line)).setError(String.format("Failed to parse line: %s", new Object[]{var3.getMessage()}));
      }
   }

   public static LogRecord makeCommit(long updateTime) {
      return new LogRecord(LogRecord.Type.COMMIT, updateTime);
   }

   public static LogRecord makeAbort(long updateTime) {
      return new LogRecord(LogRecord.Type.ABORT, updateTime);
   }

   public static LogRecord make(LogRecord.Type type, SSTable table) {
      String absoluteTablePath = absolutePath(table.descriptor.baseFilename());
      return make(type, getExistingFiles(absoluteTablePath), table.getAllFilePaths().size(), absoluteTablePath);
   }

   public static Map<SSTable, LogRecord> make(LogRecord.Type type, Iterable<SSTableReader> tables) {
      Map<String, SSTable> absolutePaths = new HashMap();
      Iterator var3 = tables.iterator();

      while(var3.hasNext()) {
         SSTableReader table = (SSTableReader)var3.next();
         absolutePaths.put(absolutePath(table.descriptor.baseFilename()), table);
      }

      Map<String, List<File>> existingFiles = getExistingFiles(absolutePaths.keySet());
      Map<SSTable, LogRecord> records = new HashMap(existingFiles.size());
      Iterator var5 = existingFiles.entrySet().iterator();

      while(var5.hasNext()) {
         Entry<String, List<File>> entry = (Entry)var5.next();
         List<File> filesOnDisk = (List)entry.getValue();
         String baseFileName = (String)entry.getKey();
         SSTable sstable = (SSTable)absolutePaths.get(baseFileName);
         records.put(sstable, make(type, filesOnDisk, sstable.getAllFilePaths().size(), baseFileName));
      }

      return records;
   }

   private static String absolutePath(String baseFilename) {
      return FileUtils.getCanonicalPath(baseFilename + '-');
   }

   public LogRecord withExistingFiles() {
      return make(this.type, this.getExistingFiles(), 0, (String)this.absolutePath.get());
   }

   public static LogRecord make(LogRecord.Type type, List<File> files, int minFiles, String absolutePath) {
      List<Long> positiveModifiedTimes = (List)files.stream().map(File::lastModified).filter((lm) -> {
         return lm.longValue() > 0L;
      }).collect(Collectors.toList());
      long lastModified = ((Long)positiveModifiedTimes.stream().reduce(Long.valueOf(0L), Long::max)).longValue();
      return new LogRecord(type, absolutePath, lastModified, Math.max(minFiles, positiveModifiedTimes.size()));
   }

   private LogRecord(LogRecord.Type type, long updateTime) {
      this(type, (String)null, updateTime, 0, 0L, (String)null);
   }

   private LogRecord(LogRecord.Type type, String absolutePath, long updateTime, int numFiles) {
      this(type, absolutePath, updateTime, numFiles, 0L, (String)null);
   }

   private LogRecord(LogRecord.Type type, String absolutePath, long updateTime, int numFiles, long checksum, String raw) {
      assert !type.hasFile() || absolutePath != null : "Expected file path for file records";

      this.type = type;
      this.absolutePath = type.hasFile()?Optional.of(absolutePath):Optional.empty();
      this.updateTime = type == LogRecord.Type.REMOVE?updateTime:0L;
      this.numFiles = type.hasFile()?numFiles:0;
      this.status = new LogRecord.Status();
      if(raw == null) {
         assert checksum == 0L;

         this.checksum = this.computeChecksum();
         this.raw = this.format();
      } else {
         this.checksum = checksum;
         this.raw = raw;
      }

   }

   LogRecord setError(String error) {
      this.status.setError(error);
      return this;
   }

   String error() {
      return (String)this.status.error.orElse("");
   }

   void setPartial() {
      this.status.partial = true;
   }

   boolean partial() {
      return this.status.partial;
   }

   boolean isValid() {
      return !this.status.hasError() && this.type != LogRecord.Type.UNKNOWN;
   }

   boolean isInvalid() {
      return !this.isValid();
   }

   boolean isInvalidOrPartial() {
      return this.isInvalid() || this.partial();
   }

   private String format() {
      return String.format("%s:[%s,%d,%d][%d]", new Object[]{this.type.toString(), this.absolutePath(), Long.valueOf(this.updateTime), Integer.valueOf(this.numFiles), Long.valueOf(this.checksum)});
   }

   public List<File> getExistingFiles() {
      assert this.absolutePath.isPresent() : "Expected a path in order to get existing files";

      return getExistingFiles((String)this.absolutePath.get());
   }

   public static List<File> getExistingFiles(String absoluteFilePath) {
      Path path = Paths.get(absoluteFilePath, new String[0]);
      File[] files = path.getParent().toFile().listFiles((dir, name) -> {
         return name.startsWith(path.getFileName().toString());
      });
      return files == null?Collections.emptyList():Arrays.asList(files);
   }

   public static Map<String, List<File>> getExistingFiles(Set<String> absoluteFilePaths) {
      Set<File> uniqueDirectories = (Set)absoluteFilePaths.stream().map((path) -> {
         return Paths.get(path, new String[0]).getParent().toFile();
      }).collect(Collectors.toSet());
      Map<String, List<File>> fileMap = new HashMap();
      FilenameFilter ff = (dir, name) -> {
         Descriptor descriptor = null;

         try {
            descriptor = Descriptor.fromFilename(new File(dir, name));
         } catch (Throwable var6) {
            ;
         }

         String absolutePath = descriptor != null?absolutePath(descriptor.baseFilename()):null;
         if(absolutePath != null && absoluteFilePaths.contains(absolutePath)) {
            ((List)fileMap.computeIfAbsent(absolutePath, (k) -> {
               return new ArrayList();
            })).add(new File(dir, name));
         }

         return false;
      };
      Iterator var4 = uniqueDirectories.iterator();

      while(var4.hasNext()) {
         File f = (File)var4.next();
         f.listFiles(ff);
      }

      return fileMap;
   }

   public boolean isFinal() {
      return this.type.isFinal();
   }

   String fileName() {
      return this.absolutePath.isPresent()?Paths.get((String)this.absolutePath.get(), new String[0]).getFileName().toString():"";
   }

   boolean isInFolder(Path folder) {
      return this.absolutePath.isPresent()?FileUtils.isContained(folder.toFile(), Paths.get((String)this.absolutePath.get(), new String[0]).toFile()):false;
   }

   String absolutePath() {
      return this.absolutePath.isPresent()?(String)this.absolutePath.get():"";
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.type, this.absolutePath, Integer.valueOf(this.numFiles), Long.valueOf(this.updateTime)});
   }

   public boolean equals(Object obj) {
      if(!(obj instanceof LogRecord)) {
         return false;
      } else {
         LogRecord other = (LogRecord)obj;
         return this.type == other.type && this.absolutePath.equals(other.absolutePath) && this.numFiles == other.numFiles && this.updateTime == other.updateTime;
      }
   }

   public String toString() {
      return this.raw;
   }

   long computeChecksum() {
      CRC32 crc32 = new CRC32();
      crc32.update(this.absolutePath().getBytes(FileUtils.CHARSET));
      crc32.update(this.type.toString().getBytes(FileUtils.CHARSET));
      FBUtilities.updateChecksumInt(crc32, (int)this.updateTime);
      FBUtilities.updateChecksumInt(crc32, (int)(this.updateTime >>> 32));
      FBUtilities.updateChecksumInt(crc32, this.numFiles);
      return crc32.getValue() & 9223372036854775807L;
   }

   LogRecord asType(LogRecord.Type type) {
      return new LogRecord(type, (String)this.absolutePath.orElse((Object)null), this.updateTime, this.numFiles);
   }

   public static final class Status {
      Optional<String> error = Optional.empty();
      boolean partial = false;
      LogRecord onDiskRecord;

      public Status() {
      }

      void setError(String error) {
         if(!this.error.isPresent()) {
            this.error = Optional.of(error);
         }

      }

      boolean hasError() {
         return this.error.isPresent();
      }
   }

   public static enum Type {
      UNKNOWN,
      ADD,
      REMOVE,
      COMMIT,
      ABORT;

      private Type() {
      }

      public static LogRecord.Type fromPrefix(String prefix) {
         return valueOf(prefix.toUpperCase());
      }

      public boolean hasFile() {
         return this == ADD || this == REMOVE;
      }

      public boolean matches(LogRecord record) {
         return this == record.type;
      }

      public boolean isFinal() {
         return this == COMMIT || this == ABORT;
      }
   }
}
