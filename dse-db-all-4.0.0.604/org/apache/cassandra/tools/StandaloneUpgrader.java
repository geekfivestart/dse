package org.apache.cassandra.tools;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.Upgrader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

public class StandaloneUpgrader {
   private static final String TOOL_NAME = "sstableupgrade";
   private static final String DEBUG_OPTION = "debug";
   private static final String HELP_OPTION = "help";
   private static final String KEEP_SOURCE = "keep-source";

   public StandaloneUpgrader() {
   }

   public static void main(String[] args) {
      StandaloneUpgrader.Options options = StandaloneUpgrader.Options.parseArgs(args);
      Util.initDatabaseDescriptor();

      try {
         Schema.instance.loadFromDisk(false);
         if(Schema.instance.getTableMetadataRef(options.keyspace, options.cf) == null) {
            throw new IllegalArgumentException(String.format("Unknown keyspace/table %s.%s", new Object[]{options.keyspace, options.cf}));
         } else {
            Keyspace keyspace = Keyspace.openWithoutSSTables(options.keyspace);
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(options.cf);
            OutputHandler handler = new OutputHandler.SystemOutput(false, options.debug);
            Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW);
            if(options.snapshot != null) {
               lister.onlyBackups(true).snapshots(options.snapshot);
            } else {
               lister.includeBackups(false);
            }

            Collection<SSTableReader> readers = new ArrayList();
            Iterator var7 = lister.list().entrySet().iterator();

            while(true) {
               Entry entry;
               Set components;
               do {
                  if(!var7.hasNext()) {
                     int numSSTables = readers.size();
                     handler.output("Found " + numSSTables + " sstables that need upgrading.");
                     Iterator var40 = readers.iterator();

                     while(var40.hasNext()) {
                        SSTableReader sstable = (SSTableReader)var40.next();

                        try {
                           LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.UPGRADE_SSTABLES, sstable);
                           Throwable var11 = null;

                           try {
                              Upgrader upgrader = new Upgrader(cfs, txn, handler);
                              upgrader.upgrade(options.keepSource);
                           } catch (Throwable var33) {
                              var11 = var33;
                              throw var33;
                           } finally {
                              if(txn != null) {
                                 if(var11 != null) {
                                    try {
                                       txn.close();
                                    } catch (Throwable var32) {
                                       var11.addSuppressed(var32);
                                    }
                                 } else {
                                    txn.close();
                                 }
                              }

                           }
                        } catch (Exception var35) {
                           System.err.println(String.format("Error upgrading %s: %s", new Object[]{sstable, var35.getMessage()}));
                           if(options.debug) {
                              var35.printStackTrace(System.err);
                           }
                        } finally {
                           sstable.selfRef().ensureReleased();
                        }
                     }

                     CompactionManager.instance.finishCompactionsAndShutdown(5L, TimeUnit.MINUTES);
                     LifecycleTransaction.waitForDeletions();
                     System.exit(0);
                     return;
                  }

                  entry = (Entry)var7.next();
                  components = (Set)entry.getValue();
               } while(!components.contains(Component.DATA));

               try {
                  SSTableReader sstable = SSTableReader.openNoValidation((Descriptor)entry.getKey(), components, cfs);
                  if(sstable.descriptor.version.equals(SSTableFormat.Type.current().info.getLatestVersion())) {
                     sstable.selfRef().release();
                  } else {
                     readers.add(sstable);
                  }
               } catch (Exception var37) {
                  JVMStabilityInspector.inspectThrowable(var37);
                  System.err.println(String.format("Error Loading %s: %s", new Object[]{entry.getKey(), var37.getMessage()}));
                  if(options.debug) {
                     var37.printStackTrace(System.err);
                  }
               }
            }
         }
      } catch (Exception var38) {
         System.err.println(var38.getMessage());
         if(options.debug) {
            var38.printStackTrace(System.err);
         }

         System.exit(1);
      }
   }

   private static class Options {
      public final String keyspace;
      public final String cf;
      public final String snapshot;
      public boolean debug;
      public boolean keepSource;

      private Options(String keyspace, String cf, String snapshot) {
         this.keyspace = keyspace;
         this.cf = cf;
         this.snapshot = snapshot;
      }

      public static StandaloneUpgrader.Options parseArgs(String[] cmdArgs) {
         CommandLineParser parser = new GnuParser();
         BulkLoader.CmdLineOptions options = getCmdLineOptions();

         try {
            CommandLine cmd = parser.parse(options, cmdArgs, false);
            if(cmd.hasOption("help")) {
               printUsage(options);
               System.exit(0);
            }

            String[] args = cmd.getArgs();
            String keyspace;
            if(args.length >= 4 || args.length < 2) {
               keyspace = args.length < 2?"Missing arguments":"Too many arguments";
               errorMsg(keyspace, options);
               System.exit(1);
            }

            keyspace = args[0];
            String cf = args[1];
            String snapshot = null;
            if(args.length == 3) {
               snapshot = args[2];
            }

            StandaloneUpgrader.Options opts = new StandaloneUpgrader.Options(keyspace, cf, snapshot);
            opts.debug = cmd.hasOption("debug");
            opts.keepSource = cmd.hasOption("keep-source");
            return opts;
         } catch (ParseException var9) {
            errorMsg(var9.getMessage(), options);
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
         options.addOption("h", "help", "display this help message");
         options.addOption("k", "keep-source", "do not delete the source sstables");
         return options;
      }

      public static void printUsage(BulkLoader.CmdLineOptions options) {
         String usage = String.format("%s [options] <keyspace> <cf> [snapshot]", new Object[]{"sstableupgrade"});
         StringBuilder header = new StringBuilder();
         header.append("--\n");
         header.append("Upgrade the sstables in the given cf (or snapshot) to the current version of Cassandra.");
         header.append("This operation will rewrite the sstables in the specified cf to match the ");
         header.append("currently installed version of Cassandra.\n");
         header.append("The snapshot option will only upgrade the specified snapshot. Upgrading ");
         header.append("snapshots is required before attempting to restore a snapshot taken in a ");
         header.append("major version older than the major version Cassandra is currently running. ");
         header.append("This will replace the files in the given snapshot as well as break any ");
         header.append("hard links to live sstables.");
         header.append("\n--\n");
         header.append("Options are:");
         (new HelpFormatter()).printHelp(usage, header.toString(), options, "");
      }
   }
}
