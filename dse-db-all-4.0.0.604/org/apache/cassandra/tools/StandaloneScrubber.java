package org.apache.cassandra.tools;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionStrategyManager;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.db.compaction.LeveledManifest;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.Scrubber;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Component;
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

public class StandaloneScrubber {
   public static final String REINSERT_OVERFLOWED_TTL_OPTION_DESCRIPTION = "Rewrites rows with overflowed expiration date affected by CASSANDRA-14092 with the maximum supported expiration date of 2038-01-19T03:14:06+00:00. The rows are rewritten with the original timestamp incremented by one millisecond to override/supersede any potential tombstone that may have been generated during compaction of the affected rows.";
   private static final String TOOL_NAME = "sstablescrub";
   private static final String VERBOSE_OPTION = "verbose";
   private static final String DEBUG_OPTION = "debug";
   private static final String HELP_OPTION = "help";
   private static final String MANIFEST_CHECK_OPTION = "manifest-check";
   private static final String SKIP_CORRUPTED_OPTION = "skip-corrupted";
   private static final String NO_VALIDATE_OPTION = "no-validate";
   private static final String REINSERT_OVERFLOWED_TTL_OPTION = "reinsert-overflowed-ttl";

   public StandaloneScrubber() {
   }

   public static void main(String[] args) {
      StandaloneScrubber.Options options = StandaloneScrubber.Options.parseArgs(args);
      Util.initDatabaseDescriptor();

      try {
         Schema.instance.loadFromDisk(false);
         if(Schema.instance.getKeyspaceMetadata(options.keyspaceName) == null) {
            throw new IllegalArgumentException(String.format("Unknown keyspace %s", new Object[]{options.keyspaceName}));
         } else {
            Keyspace keyspace = Keyspace.openWithoutSSTables(options.keyspaceName);
            ColumnFamilyStore cfs = null;
            Iterator var4 = keyspace.getValidColumnFamilies(true, false, new String[]{options.cfName}).iterator();

            while(var4.hasNext()) {
               ColumnFamilyStore c = (ColumnFamilyStore)var4.next();
               if(c.name.equals(options.cfName)) {
                  cfs = c;
                  break;
               }
            }

            if(cfs == null) {
               throw new IllegalArgumentException(String.format("Unknown table %s.%s", new Object[]{options.keyspaceName, options.cfName}));
            } else {
               String snapshotName = "pre-scrub-" + System.currentTimeMillis();
               OutputHandler handler = new OutputHandler.SystemOutput(options.verbose, options.debug);
               Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW).skipTemporary(true);
               List<SSTableReader> sstables = new ArrayList();
               Iterator var8 = lister.list().entrySet().iterator();

               while(true) {
                  Entry entry;
                  Set components;
                  do {
                     if(!var8.hasNext()) {
                        System.out.println(String.format("Pre-scrub sstables snapshotted into snapshot %s", new Object[]{snapshotName}));
                        if(!options.manifestCheckOnly) {
                           var8 = sstables.iterator();

                           while(var8.hasNext()) {
                              SSTableReader sstable = (SSTableReader)var8.next();

                              try {
                                 LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.SCRUB, sstable);
                                 Throwable var56 = null;

                                 try {
                                    txn.obsoleteOriginals();

                                    try {
                                       Scrubber scrubber = new Scrubber(cfs, txn, options.skipCorrupted, handler, !options.noValidate, options.reinserOverflowedTTL);
                                       Throwable var13 = null;

                                       try {
                                          scrubber.scrub();
                                       } catch (Throwable var44) {
                                          var13 = var44;
                                          throw var44;
                                       } finally {
                                          if(scrubber != null) {
                                             if(var13 != null) {
                                                try {
                                                   scrubber.close();
                                                } catch (Throwable var43) {
                                                   var13.addSuppressed(var43);
                                                }
                                             } else {
                                                scrubber.close();
                                             }
                                          }

                                       }
                                    } catch (Throwable var47) {
                                       if(!cfs.rebuildOnFailedScrub(var47)) {
                                          System.out.println(var47.getMessage());
                                          throw var47;
                                       }
                                    }
                                 } catch (Throwable var48) {
                                    var56 = var48;
                                    throw var48;
                                 } finally {
                                    if(txn != null) {
                                       if(var56 != null) {
                                          try {
                                             txn.close();
                                          } catch (Throwable var42) {
                                             var56.addSuppressed(var42);
                                          }
                                       } else {
                                          txn.close();
                                       }
                                    }

                                 }
                              } catch (Exception var50) {
                                 System.err.println(String.format("Error scrubbing %s: %s", new Object[]{sstable, var50.getMessage()}));
                                 var50.printStackTrace(System.err);
                              }
                           }
                        }

                        checkManifest(cfs.getCompactionStrategyManager(), cfs, sstables);
                        CompactionManager.instance.finishCompactionsAndShutdown(5L, TimeUnit.MINUTES);
                        LifecycleTransaction.waitForDeletions();
                        System.exit(0);
                        return;
                     }

                     entry = (Entry)var8.next();
                     components = (Set)entry.getValue();
                  } while(!components.contains(Component.DATA));

                  try {
                     SSTableReader sstable = SSTableReader.openNoValidation((Descriptor)entry.getKey(), components, cfs);
                     sstables.add(sstable);
                     File snapshotDirectory = Directories.getSnapshotDirectory(sstable.descriptor, snapshotName);
                     sstable.createLinks(snapshotDirectory.getPath());
                  } catch (Exception var46) {
                     JVMStabilityInspector.inspectThrowable(var46);
                     System.err.println(String.format("Error Loading %s: %s", new Object[]{entry.getKey(), var46.getMessage()}));
                     if(options.debug) {
                        var46.printStackTrace(System.err);
                     }
                  }
               }
            }
         }
      } catch (Exception var51) {
         System.err.println(var51.getMessage());
         if(options.debug) {
            var51.printStackTrace(System.err);
         }

         System.exit(1);
      }
   }

   private static void checkManifest(CompactionStrategyManager strategyManager, ColumnFamilyStore cfs, Collection<SSTableReader> sstables) {
      if(strategyManager.getCompactionParams().klass().equals(LeveledCompactionStrategy.class)) {
         int maxSizeInMB = (int)(cfs.getCompactionStrategyManager().getMaxSSTableBytes() / 1048576L);
         System.out.println("Checking leveled manifest");
         Predicate<SSTableReader> repairedPredicate = new Predicate<SSTableReader>() {
            public boolean apply(SSTableReader sstable) {
               return sstable.isRepaired();
            }
         };
         List<SSTableReader> repaired = Lists.newArrayList(Iterables.filter(sstables, repairedPredicate));
         List<SSTableReader> unRepaired = Lists.newArrayList(Iterables.filter(sstables, Predicates.not(repairedPredicate)));
         LeveledManifest repairedManifest = LeveledManifest.create(cfs, maxSizeInMB, cfs.getLevelFanoutSize(), repaired);

         for(int i = 1; i < repairedManifest.getLevelCount(); ++i) {
            repairedManifest.repairOverlappingSSTables(i);
         }

         LeveledManifest unRepairedManifest = LeveledManifest.create(cfs, maxSizeInMB, cfs.getLevelFanoutSize(), unRepaired);

         for(int i = 1; i < unRepairedManifest.getLevelCount(); ++i) {
            unRepairedManifest.repairOverlappingSSTables(i);
         }
      }

   }

   private static class Options {
      public final String keyspaceName;
      public final String cfName;
      public boolean debug;
      public boolean verbose;
      public boolean manifestCheckOnly;
      public boolean skipCorrupted;
      public boolean noValidate;
      public boolean reinserOverflowedTTL;

      private Options(String keyspaceName, String cfName) {
         this.keyspaceName = keyspaceName;
         this.cfName = cfName;
      }

      public static StandaloneScrubber.Options parseArgs(String[] cmdArgs) {
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
            StandaloneScrubber.Options opts = new StandaloneScrubber.Options(keyspaceName, cfName);
            opts.debug = cmd.hasOption("debug");
            opts.verbose = cmd.hasOption("verbose");
            opts.manifestCheckOnly = cmd.hasOption("manifest-check");
            opts.skipCorrupted = cmd.hasOption("skip-corrupted");
            opts.noValidate = cmd.hasOption("no-validate");
            opts.reinserOverflowedTTL = cmd.hasOption("reinsert-overflowed-ttl");
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
         options.addOption("v", "verbose", "verbose output");
         options.addOption("h", "help", "display this help message");
         options.addOption("m", "manifest-check", "only check and repair the leveled manifest, without actually scrubbing the sstables");
         options.addOption("s", "skip-corrupted", "skip corrupt rows in counter tables");
         options.addOption("n", "no-validate", "do not validate columns using column validator");
         options.addOption("r", "reinsert-overflowed-ttl", "Rewrites rows with overflowed expiration date affected by CASSANDRA-14092 with the maximum supported expiration date of 2038-01-19T03:14:06+00:00. The rows are rewritten with the original timestamp incremented by one millisecond to override/supersede any potential tombstone that may have been generated during compaction of the affected rows.");
         return options;
      }

      public static void printUsage(BulkLoader.CmdLineOptions options) {
         String usage = String.format("%s [options] <keyspace> <column_family>", new Object[]{"sstablescrub"});
         StringBuilder header = new StringBuilder();
         header.append("--\n");
         header.append("Scrub the sstable for the provided table.");
         header.append("\n--\n");
         header.append("Options are:");
         (new HelpFormatter()).printHelp(usage, header.toString(), options, "");
      }
   }
}
