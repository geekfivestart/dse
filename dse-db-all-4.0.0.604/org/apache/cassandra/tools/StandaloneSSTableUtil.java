package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.BiPredicate;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

public class StandaloneSSTableUtil {
   private static final String TOOL_NAME = "sstableutil";
   private static final String TYPE_OPTION = "type";
   private static final String OP_LOG_OPTION = "oplog";
   private static final String VERBOSE_OPTION = "verbose";
   private static final String DEBUG_OPTION = "debug";
   private static final String HELP_OPTION = "help";
   private static final String CLEANUP_OPTION = "cleanup";

   public StandaloneSSTableUtil() {
   }

   public static void main(String[] args) {
      StandaloneSSTableUtil.Options options = StandaloneSSTableUtil.Options.parseArgs(args);

      try {
         Util.initDatabaseDescriptor();
         Schema.instance.loadFromDisk(false);
         TableMetadata metadata = Schema.instance.getTableMetadata(options.keyspaceName, options.cfName);
         if(metadata == null) {
            throw new IllegalArgumentException(String.format("Unknown keyspace/table %s.%s", new Object[]{options.keyspaceName, options.cfName}));
         }

         OutputHandler handler = new OutputHandler.SystemOutput(options.verbose, options.debug);
         if(options.cleanup) {
            handler.output("Cleanuping up...");
            LifecycleTransaction.removeUnfinishedLeftovers(metadata);
         } else {
            handler.output("Listing files...");
            listFiles(options, metadata, handler);
         }

         System.exit(0);
      } catch (Exception var4) {
         System.err.println(var4.getMessage());
         if(options.debug) {
            var4.printStackTrace(System.err);
         }

         System.exit(1);
      }

   }

   private static void listFiles(StandaloneSSTableUtil.Options options, TableMetadata metadata, OutputHandler handler) throws IOException {
      Directories directories = new Directories(metadata, ColumnFamilyStore.getInitialDirectories());
      Iterator var4 = directories.getCFDirectories().iterator();

      while(var4.hasNext()) {
         File dir = (File)var4.next();
         Iterator var6 = LifecycleTransaction.getFiles(dir.toPath(), getFilter(options), Directories.OnTxnErr.THROW).iterator();

         while(var6.hasNext()) {
            File file = (File)var6.next();
            handler.output(file.getCanonicalPath());
         }
      }

   }

   private static BiPredicate<File, Directories.FileType> getFilter(Options options) {
      return (file, type) -> {
         switch (type) {
            case FINAL: {
               return options.type != Options.FileType.TMP;
            }
            case TEMPORARY: {
               return options.type != Options.FileType.FINAL;
            }
            case TXN_LOG: {
               return options.oplogs;
            }
         }
         throw new AssertionError();
      };
   }

   private static class Options {
      public final String keyspaceName;
      public final String cfName;
      public boolean debug;
      public boolean verbose;
      public boolean oplogs;
      public boolean cleanup;
      public StandaloneSSTableUtil.Options.FileType type;

      private Options(String keyspaceName, String cfName) {
         this.keyspaceName = keyspaceName;
         this.cfName = cfName;
      }

      public static StandaloneSSTableUtil.Options parseArgs(String[] cmdArgs) {
         CommandLineParser parser = new GnuParser();
         BulkLoader.CmdLineOptions options = getCmdLineOptions();

         try {
            CommandLine cmd = parser.parse(options, cmdArgs, false);
            if(cmd.hasOption("help")) {
               printUsage(options);
               System.exit(0);
            }

            String[] args = cmd.getArgs();
            String keyspaceName;
            if(args.length != 2) {
               keyspaceName = args.length < 2?"Missing arguments":"Too many arguments";
               System.err.println(keyspaceName);
               printUsage(options);
               System.exit(1);
            }

            keyspaceName = args[0];
            String cfName = args[1];
            StandaloneSSTableUtil.Options opts = new StandaloneSSTableUtil.Options(keyspaceName, cfName);
            opts.debug = cmd.hasOption("debug");
            opts.verbose = cmd.hasOption("verbose");
            opts.type = StandaloneSSTableUtil.Options.FileType.fromOption(cmd.getOptionValue("type"));
            opts.oplogs = cmd.hasOption("oplog");
            opts.cleanup = cmd.hasOption("cleanup");
            return opts;
         } catch (ParseException var8) {
            errorMsg(var8.getMessage(), options);
            return null;
         }
      }

      private static void errorMsg(String msg, BulkLoader.CmdLineOptions options) {
         System.err.println(msg);
         printUsage(options);
         System.exit(1);
      }

      private static BulkLoader.CmdLineOptions getCmdLineOptions() {
         BulkLoader.CmdLineOptions options = new BulkLoader.CmdLineOptions();
         options.addOption("c", "cleanup", "clean-up any outstanding transactions");
         options.addOption("d", "debug", "display stack traces");
         options.addOption("h", "help", "display this help message");
         options.addOption("o", "oplog", "include operation logs");
         options.addOption("t", "type", true, StandaloneSSTableUtil.Options.FileType.descr());
         options.addOption("v", "verbose", "verbose output");
         return options;
      }

      public static void printUsage(BulkLoader.CmdLineOptions options) {
         String usage = String.format("%s [options] <keyspace> <column_family>", new Object[]{"sstableutil"});
         StringBuilder header = new StringBuilder();
         header.append("--\n");
         header.append("List sstable files for the provided table.");
         header.append("\n--\n");
         header.append("Options are:");
         (new HelpFormatter()).printHelp(usage, header.toString(), options, "");
      }

      public static enum FileType {
         ALL("all", "list all files, final or temporary"),
         TMP("tmp", "list temporary files only"),
         FINAL("final", "list final files only");

         public String option;
         public String descr;

         private FileType(String option, String descr) {
            this.option = option;
            this.descr = descr;
         }

         static StandaloneSSTableUtil.Options.FileType fromOption(String option) {
            StandaloneSSTableUtil.Options.FileType[] var1 = values();
            int var2 = var1.length;

            for(int var3 = 0; var3 < var2; ++var3) {
               StandaloneSSTableUtil.Options.FileType fileType = var1[var3];
               if(fileType.option.equals(option)) {
                  return fileType;
               }
            }

            return ALL;
         }

         static String descr() {
            StringBuilder str = new StringBuilder();
            StandaloneSSTableUtil.Options.FileType[] var1 = values();
            int var2 = var1.length;

            for(int var3 = 0; var3 < var2; ++var3) {
               StandaloneSSTableUtil.Options.FileType fileType = var1[var3];
               str.append(fileType.option);
               str.append(" (");
               str.append(fileType.descr);
               str.append("), ");
            }

            return str.toString();
         }
      }
   }
}
