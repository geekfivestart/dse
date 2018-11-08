package org.apache.cassandra.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.SSTableSplitter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

public class StandaloneSplitter {
   public static final int DEFAULT_SSTABLE_SIZE = 50;
   private static final String TOOL_NAME = "sstablessplit";
   private static final String DEBUG_OPTION = "debug";
   private static final String HELP_OPTION = "help";
   private static final String NO_SNAPSHOT_OPTION = "no-snapshot";
   private static final String SIZE_OPTION = "size";

   public StandaloneSplitter() {
   }

   public static void main(String[] args) {
      StandaloneSplitter.Options options = StandaloneSplitter.Options.parseArgs(args);
      Util.initDatabaseDescriptor();

      try {
         Schema.instance.loadFromDisk(false);
         String ksName = null;
         String cfName = null;
         Map<Descriptor, Set<Component>> parsedFilenames = new HashMap();
         Iterator var5 = options.filenames.iterator();

         while(var5.hasNext()) {
            String filename = (String)var5.next();
            File file = new File(filename);
            if(!file.exists()) {
               System.out.println("Skipping inexisting file " + file);
            } else {
               Descriptor desc = SSTable.tryDescriptorFromFilename(file);
               if(desc == null) {
                  System.out.println("Skipping non sstable file " + file);
               } else {
                  if(ksName == null) {
                     ksName = desc.ksname;
                  } else if(!ksName.equals(desc.ksname)) {
                     throw new IllegalArgumentException("All sstables must be part of the same keyspace");
                  }

                  if(cfName == null) {
                     cfName = desc.cfname;
                  } else if(!cfName.equals(desc.cfname)) {
                     throw new IllegalArgumentException("All sstables must be part of the same table");
                  }

                  Set<Component> components = SSTableLoader.mainComponentsPresent(desc);
                  parsedFilenames.put(desc, components);
               }
            }
         }

         if(ksName == null || cfName == null) {
            System.err.println("No valid sstables to split");
            System.exit(1);
         }

         Keyspace keyspace = Keyspace.openWithoutSSTables(ksName);
         ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
         String snapshotName = "pre-split-" + System.currentTimeMillis();
         List<SSTableReader> sstables = new ArrayList();
         Iterator var33 = parsedFilenames.entrySet().iterator();

         while(var33.hasNext()) {
            Entry fn = (Entry)var33.next();

            try {
               SSTableReader sstable = SSTableReader.openNoValidation((Descriptor)fn.getKey(), (Set)fn.getValue(), cfs);
               if(!isSSTableLargerEnough(sstable, options.sizeInMB)) {
                  System.out.println(String.format("Skipping %s: it's size (%.3f MB) is less than the split size (%d MB)", new Object[]{sstable.getFilename(), Double.valueOf((double)sstable.onDiskLength() * 1.0D / 1024.0D / 1024.0D), Integer.valueOf(options.sizeInMB)}));
               } else {
                  sstables.add(sstable);
                  if(options.snapshot) {
                     File snapshotDirectory = Directories.getSnapshotDirectory(sstable.descriptor, snapshotName);
                     sstable.createLinks(snapshotDirectory.getPath());
                  }
               }
            } catch (Exception var25) {
               JVMStabilityInspector.inspectThrowable(var25);
               System.err.println(String.format("Error Loading %s: %s", new Object[]{fn.getKey(), var25.getMessage()}));
               if(options.debug) {
                  var25.printStackTrace(System.err);
               }
            }
         }

         if(sstables.isEmpty()) {
            System.out.println("No sstables needed splitting.");
            System.exit(0);
         }

         if(options.snapshot) {
            System.out.println(String.format("Pre-split sstables snapshotted into snapshot %s", new Object[]{snapshotName}));
         }

         var33 = sstables.iterator();

         while(var33.hasNext()) {
            SSTableReader sstable = (SSTableReader)var33.next();

            try {
               LifecycleTransaction transaction = LifecycleTransaction.offline(OperationType.UNKNOWN, sstable);
               Throwable var36 = null;

               try {
                  (new SSTableSplitter(cfs, transaction, options.sizeInMB)).split();
               } catch (Throwable var24) {
                  var36 = var24;
                  throw var24;
               } finally {
                  if(transaction != null) {
                     if(var36 != null) {
                        try {
                           transaction.close();
                        } catch (Throwable var23) {
                           var36.addSuppressed(var23);
                        }
                     } else {
                        transaction.close();
                     }
                  }

               }
            } catch (Exception var27) {
               System.err.println(String.format("Error splitting %s: %s", new Object[]{sstable, var27.getMessage()}));
               if(options.debug) {
                  var27.printStackTrace(System.err);
               }

               sstable.selfRef().release();
            }
         }

         CompactionManager.instance.finishCompactionsAndShutdown(5L, TimeUnit.MINUTES);
         LifecycleTransaction.waitForDeletions();
         System.exit(0);
      } catch (Exception var28) {
         System.err.println(var28.getMessage());
         if(options.debug) {
            var28.printStackTrace(System.err);
         }

         System.exit(1);
      }

   }

   private static boolean isSSTableLargerEnough(SSTableReader sstable, int sizeInMB) {
      return sstable.onDiskLength() > (long)sizeInMB * 1024L * 1024L;
   }

   private static class Options {
      public final List<String> filenames;
      public boolean debug;
      public boolean snapshot;
      public int sizeInMB;

      private Options(List<String> filenames) {
         this.filenames = filenames;
      }

      public static StandaloneSplitter.Options parseArgs(String[] cmdArgs) {
         CommandLineParser parser = new GnuParser();
         BulkLoader.CmdLineOptions options = getCmdLineOptions();

         try {
            CommandLine cmd = parser.parse(options, cmdArgs, false);
            if(cmd.hasOption("help")) {
               printUsage(options);
               System.exit(0);
            }

            String[] args = cmd.getArgs();
            if(args.length == 0) {
               System.err.println("No sstables to split");
               printUsage(options);
               System.exit(1);
            }

            StandaloneSplitter.Options opts = new StandaloneSplitter.Options(Arrays.asList(args));
            opts.debug = cmd.hasOption("debug");
            opts.snapshot = !cmd.hasOption("no-snapshot");
            opts.sizeInMB = 50;
            if(cmd.hasOption("size")) {
               opts.sizeInMB = Integer.parseInt(cmd.getOptionValue("size"));
            }

            return opts;
         } catch (ParseException var6) {
            errorMsg(var6.getMessage(), options);
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
         options.addOption((String)null, "no-snapshot", "don't snapshot the sstables before splitting");
         options.addOption("s", "size", "size", "maximum size in MB for the output sstables (default: 50)");
         return options;
      }

      public static void printUsage(BulkLoader.CmdLineOptions options) {
         String usage = String.format("%s [options] <filename> [<filename>]*", new Object[]{"sstablessplit"});
         StringBuilder header = new StringBuilder();
         header.append("--\n");
         header.append("Split the provided sstables files in sstables of maximum provided file size (see option --size).");
         header.append("\n--\n");
         header.append("Options are:");
         (new HelpFormatter()).printHelp(usage, header.toString(), options, "");
      }
   }
}
