package org.apache.cassandra.db.commitlog;

import com.google.common.base.Strings;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitLogArchiver {
   private static final Logger logger = LoggerFactory.getLogger(CommitLogArchiver.class);
   public static final SimpleDateFormat format = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss");
   private static final String DELIMITER = ",";
   private static final Pattern NAME = Pattern.compile("%name");
   private static final Pattern PATH = Pattern.compile("%path");
   private static final Pattern FROM = Pattern.compile("%from");
   private static final Pattern TO = Pattern.compile("%to");
   public final Map<String, Future<?>> archivePending = new ConcurrentHashMap();
   private final ExecutorService executor;
   final String archiveCommand;
   final String restoreCommand;
   final String restoreDirectories;
   public long restorePointInTime;
   public final TimeUnit precision;

   public CommitLogArchiver(String archiveCommand, String restoreCommand, String restoreDirectories, long restorePointInTime, TimeUnit precision) {
      this.archiveCommand = archiveCommand;
      this.restoreCommand = restoreCommand;
      this.restoreDirectories = restoreDirectories;
      this.restorePointInTime = restorePointInTime;
      this.precision = precision;
      this.executor = !Strings.isNullOrEmpty(archiveCommand)?new JMXEnabledThreadPoolExecutor("CommitLogArchiver"):null;
   }

   public static CommitLogArchiver disabled() {
      return new CommitLogArchiver((String)null, (String)null, (String)null, 9223372036854775807L, TimeUnit.MICROSECONDS);
   }

   public static CommitLogArchiver construct() {
      Properties commitlog_commands = new Properties();

      try {
         InputStream stream = CommitLogArchiver.class.getClassLoader().getResourceAsStream("commitlog_archiving.properties");
         Throwable var2 = null;

         CommitLogArchiver var30;
         try {
            if(stream == null) {
               logger.trace("No commitlog_archiving properties found; archive + pitr will be disabled");
               CommitLogArchiver var26 = disabled();
               return var26;
            }

            commitlog_commands.load(stream);
            String archiveCommand = commitlog_commands.getProperty("archive_command");
            String restoreCommand = commitlog_commands.getProperty("restore_command");
            String restoreDirectories = commitlog_commands.getProperty("restore_directories");
            if(restoreDirectories != null && !restoreDirectories.isEmpty()) {
               String[] var6 = restoreDirectories.split(",");
               int var7 = var6.length;

               for(int var8 = 0; var8 < var7; ++var8) {
                  String dir = var6[var8];
                  File directory = new File(dir);
                  if(!directory.exists() && !directory.mkdir()) {
                     throw new RuntimeException("Unable to create directory: " + dir);
                  }
               }
            }

            String targetTime = commitlog_commands.getProperty("restore_point_in_time");
            TimeUnit precision = TimeUnit.valueOf(commitlog_commands.getProperty("precision", "MICROSECONDS"));

            long restorePointInTime;
            try {
               restorePointInTime = Strings.isNullOrEmpty(targetTime)?9223372036854775807L:format.parse(targetTime).getTime();
            } catch (ParseException var22) {
               throw new RuntimeException("Unable to parse restore target time", var22);
            }

            var30 = new CommitLogArchiver(archiveCommand, restoreCommand, restoreDirectories, restorePointInTime, precision);
         } catch (Throwable var23) {
            var2 = var23;
            throw var23;
         } finally {
            if(stream != null) {
               if(var2 != null) {
                  try {
                     stream.close();
                  } catch (Throwable var21) {
                     var2.addSuppressed(var21);
                  }
               } else {
                  stream.close();
               }
            }

         }

         return var30;
      } catch (IOException var25) {
         throw new RuntimeException("Unable to load commitlog_archiving.properties", var25);
      }
   }

   public void maybeArchive(final CommitLogSegment segment) {
      if(!Strings.isNullOrEmpty(this.archiveCommand)) {
         this.archivePending.put(segment.getName(), this.executor.submit(new WrappedRunnable() {
            protected void runMayThrow() throws IOException {
               segment.waitForFinalSync();
               String command = CommitLogArchiver.NAME.matcher(CommitLogArchiver.this.archiveCommand).replaceAll(Matcher.quoteReplacement(segment.getName()));
               command = CommitLogArchiver.PATH.matcher(command).replaceAll(Matcher.quoteReplacement(segment.getPath()));
               CommitLogArchiver.this.exec(command);
            }
         }));
      }
   }

   public void maybeArchive(final String path, final String name) {
      if(!Strings.isNullOrEmpty(this.archiveCommand)) {
         this.archivePending.put(name, this.executor.submit(new Runnable() {
            public void run() {
               try {
                  String command = CommitLogArchiver.NAME.matcher(CommitLogArchiver.this.archiveCommand).replaceAll(Matcher.quoteReplacement(name));
                  command = CommitLogArchiver.PATH.matcher(command).replaceAll(Matcher.quoteReplacement(path));
                  CommitLogArchiver.this.exec(command);
               } catch (IOException var2) {
                  CommitLogArchiver.logger.warn("Archiving file {} failed, file may have already been archived.", name, var2);
               }

            }
         }));
      }
   }

   public boolean maybeWaitForArchiving(String name) {
      Future<?> f = (Future)this.archivePending.remove(name);
      if(f == null) {
         return true;
      } else {
         try {
            f.get();
            return true;
         } catch (InterruptedException var4) {
            throw new AssertionError(var4);
         } catch (ExecutionException var5) {
            if(var5.getCause() instanceof RuntimeException && var5.getCause().getCause() instanceof IOException) {
               logger.error("Looks like the archiving of file {} failed earlier, cassandra is going to ignore this segment for now.", name, var5.getCause().getCause());
               return false;
            } else {
               throw new RuntimeException(var5);
            }
         }
      }
   }

   public void maybeRestoreArchive() {
      if(!Strings.isNullOrEmpty(this.restoreDirectories)) {
         String[] var1 = this.restoreDirectories.split(",");
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            String dir = var1[var3];
            File[] files = (new File(dir)).listFiles();
            if(files == null) {
               throw new RuntimeException("Unable to list directory " + dir);
            }

            File[] var6 = files;
            int var7 = files.length;

            for(int var8 = 0; var8 < var7; ++var8) {
               File fromFile = var6[var8];
               CommitLogDescriptor fromHeader = CommitLogDescriptor.fromHeader(fromFile, DatabaseDescriptor.getEncryptionContext());
               CommitLogDescriptor fromName = CommitLogDescriptor.isValid(fromFile.getName())?CommitLogDescriptor.fromFileName(fromFile.getName()):null;
               if(fromHeader == null && fromName == null) {
                  throw new IllegalStateException("Cannot safely construct descriptor for segment, either from its name or its header: " + fromFile.getPath());
               }

               if(fromHeader != null && fromName != null && !fromHeader.equalsIgnoringCompression(fromName)) {
                  throw new IllegalStateException(String.format("Cannot safely construct descriptor for segment, as name and header descriptors do not match (%s vs %s): %s", new Object[]{fromHeader, fromName, fromFile.getPath()}));
               }

               if(fromName != null && fromHeader == null) {
                  throw new IllegalStateException("Cannot safely construct descriptor for segment, as name descriptor implies a version that should contain a header descriptor, but that descriptor could not be read: " + fromFile.getPath());
               }

               CommitLogDescriptor descriptor;
               if(fromHeader != null) {
                  descriptor = fromHeader;
               } else {
                  descriptor = fromName;
               }

               if(descriptor.version.compareTo(CommitLogDescriptor.current_version) > 0) {
                  throw new IllegalStateException("Unsupported commit log version: " + descriptor.version);
               }

               if(descriptor.compression != null) {
                  try {
                     CompressionParams.createCompressor(descriptor.compression);
                  } catch (ConfigurationException var16) {
                     throw new IllegalStateException("Unknown compression", var16);
                  }
               }

               File toFile = new File(DatabaseDescriptor.getCommitLogLocation(), descriptor.fileName());
               if(toFile.exists()) {
                  logger.trace("Skipping restore of archive {} as the segment already exists in the restore location {}", fromFile.getPath(), toFile.getPath());
               } else {
                  String command = FROM.matcher(this.restoreCommand).replaceAll(Matcher.quoteReplacement(fromFile.getPath()));
                  command = TO.matcher(command).replaceAll(Matcher.quoteReplacement(toFile.getPath()));

                  try {
                     this.exec(command);
                  } catch (IOException var17) {
                     throw new RuntimeException(var17);
                  }
               }
            }
         }

      }
   }

   private void exec(String command) throws IOException {
      ProcessBuilder pb = new ProcessBuilder(command.split(" "));
      pb.redirectErrorStream(true);
      FBUtilities.exec(pb);
   }

   static {
      format.setTimeZone(TimeZone.getTimeZone("GMT"));
   }
}
