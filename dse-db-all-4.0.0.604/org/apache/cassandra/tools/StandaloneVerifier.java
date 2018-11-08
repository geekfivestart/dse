package org.apache.cassandra.tools;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.Verifier;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

public class StandaloneVerifier {
   private static final String TOOL_NAME = "sstableverify";
   private static final String VERBOSE_OPTION = "verbose";
   private static final String EXTENDED_OPTION = "extended";
   private static final String DEBUG_OPTION = "debug";
   private static final String HELP_OPTION = "help";

   public StandaloneVerifier() {
   }

   public static void main(String[] args) {
      StandaloneVerifier.Options options = StandaloneVerifier.Options.parseArgs(args);
      Util.initDatabaseDescriptor();

      try {
         Schema.instance.loadFromDisk(false);
         boolean hasFailed = false;
         if(Schema.instance.getTableMetadataRef(options.keyspaceName, options.cfName) == null) {
            throw new IllegalArgumentException(String.format("Unknown keyspace/table %s.%s", new Object[]{options.keyspaceName, options.cfName}));
         } else {
            Keyspace keyspace = Keyspace.openWithoutSSTables(options.keyspaceName);
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(options.cfName);
            OutputHandler handler = new OutputHandler.SystemOutput(options.verbose, options.debug);
            Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW).skipTemporary(true);
            boolean extended = options.extended;
            List<SSTableReader> sstables = new ArrayList();
            Iterator var9 = lister.list().entrySet().iterator();

            while(true) {
               Entry entry;
               Set components;
               do {
                  if(!var9.hasNext()) {
                     var9 = sstables.iterator();

                     while(var9.hasNext()) {
                        SSTableReader sstable = (SSTableReader)var9.next();

                        try {
                           try {
                              Verifier verifier = new Verifier(cfs, sstable, handler, true);
                              Throwable var33 = null;

                              try {
                                 verifier.verify(extended);
                              } catch (Throwable var25) {
                                 var33 = var25;
                                 throw var25;
                              } finally {
                                 if(verifier != null) {
                                    if(var33 != null) {
                                       try {
                                          verifier.close();
                                       } catch (Throwable var24) {
                                          var33.addSuppressed(var24);
                                       }
                                    } else {
                                       verifier.close();
                                    }
                                 }

                              }
                           } catch (CorruptSSTableException var27) {
                              System.err.println(String.format("Error verifying %s: %s", new Object[]{sstable, var27.getMessage()}));
                              hasFailed = true;
                           }
                        } catch (Exception var28) {
                           System.err.println(String.format("Error verifying %s: %s", new Object[]{sstable, var28.getMessage()}));
                           var28.printStackTrace(System.err);
                        }
                     }

                     CompactionManager.instance.finishCompactionsAndShutdown(5L, TimeUnit.MINUTES);
                     System.exit(hasFailed?1:0);
                     return;
                  }

                  entry = (Entry)var9.next();
                  components = (Set)entry.getValue();
               } while(!components.containsAll(SSTableReader.requiredComponents((Descriptor)entry.getKey())));

               try {
                  SSTableReader sstable = SSTableReader.openNoValidation((Descriptor)entry.getKey(), components, cfs);
                  sstables.add(sstable);
               } catch (Exception var29) {
                  JVMStabilityInspector.inspectThrowable(var29);
                  System.err.println(String.format("Error Loading %s: %s", new Object[]{entry.getKey(), var29.getMessage()}));
                  if(options.debug) {
                     var29.printStackTrace(System.err);
                  }
               }
            }
         }
      } catch (Exception var30) {
         System.err.println(var30.getMessage());
         if(options.debug) {
            var30.printStackTrace(System.err);
         }

         System.exit(1);
      }
   }

   private static class Options {
      public final String keyspaceName;
      public final String cfName;
      public boolean debug;
      public boolean verbose;
      public boolean extended;

      private Options(String keyspaceName, String cfName) {
         this.keyspaceName = keyspaceName;
         this.cfName = cfName;
      }

      public static StandaloneVerifier.Options parseArgs(String[] cmdArgs) {
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
            StandaloneVerifier.Options opts = new StandaloneVerifier.Options(keyspaceName, cfName);
            opts.debug = cmd.hasOption("debug");
            opts.verbose = cmd.hasOption("verbose");
            opts.extended = cmd.hasOption("extended");
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
         options.addOption((String)null, "debug", "display stack traces");
         options.addOption("e", "extended", "extended verification");
         options.addOption("v", "verbose", "verbose output");
         options.addOption("h", "help", "display this help message");
         return options;
      }

      public static void printUsage(BulkLoader.CmdLineOptions options) {
         String usage = String.format("%s [options] <keyspace> <column_family>", new Object[]{"sstableverify"});
         StringBuilder header = new StringBuilder();
         header.append("--\n");
         header.append("Verify the sstable for the provided table.");
         header.append("\n--\n");
         header.append("Options are:");
         (new HelpFormatter()).printHelp(usage, header.toString(), options, "");
      }
   }
}
