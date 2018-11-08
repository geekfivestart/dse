package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class SSTablePartitions {
   private static final String KEY_OPTION = "k";
   private static final String EXCLUDE_KEY_OPTION = "x";
   private static final String RECURSIVE_OPTION = "r";
   private static final String SNAPSHOTS_OPTION = "s";
   private static final String BACKUPS_OPTION = "b";
   private static final String PARTITIONS_ONLY_OPTION = "y";
   private static final String SIZE_THRESHOLD_OPTION = "t";
   private static final String TOMBSTONE_THRESHOLD_OPTION = "o";
   private static final String CELL_THRESHOLD_OPTION = "c";
   private static final String CSV_OPTION = "m";
   private static final String CURRENT_TIMESTAMP_OPTION = "u";
   private static final Options options = new Options();
   private static final TableId EMPTY_TABLE_ID = TableId.fromUUID(new UUID(0L, 0L));

   public SSTablePartitions() {
   }

   public static void main(String[] args) throws ConfigurationException {
      PosixParser parser = new PosixParser();

      CommandLine cmd;
      try {
         cmd = parser.parse(options, args);
      } catch (ParseException var4) {
         System.err.println(var4.getMessage());
         printUsage();
         System.exit(1);
         return;
      }

      if(cmd.getArgs().length == 0) {
         System.err.println("You must supply at least one sstable or directory");
         printUsage();
         System.exit(1);
      }

      int ec = processArguments(cmd);
      System.exit(ec);
   }

   private static int processArguments(CommandLine cmd) {
      String[] keys = cmd.getOptionValues("k");
      HashSet<String> excludes = new HashSet(Arrays.asList(cmd.getOptionValues("x") == null?new String[0]:cmd.getOptionValues("x")));
      boolean scanRecursive = cmd.hasOption("r");
      boolean withSnapshots = cmd.hasOption("s");
      boolean withBackups = cmd.hasOption("b");
      boolean csv = cmd.hasOption("m");
      boolean partitionsOnly = cmd.hasOption("y");
      long sizeThreshold = 9223372036854775807L;
      int cellCountThreshold = 2147483647;
      int tombstoneCountThreshold = 2147483647;
      long currentTime = System.currentTimeMillis() / 1000L;

      try {
         if(cmd.hasOption("t")) {
            sizeThreshold = Long.parseLong(cmd.getOptionValue("t"));
         }

         if(cmd.hasOption("c")) {
            cellCountThreshold = Integer.parseInt(cmd.getOptionValue("c"));
         }

         if(cmd.hasOption("o")) {
            tombstoneCountThreshold = Integer.parseInt(cmd.getOptionValue("o"));
         }

         if(cmd.hasOption("u")) {
            currentTime = (long)Integer.parseInt(cmd.getOptionValue("u"));
         }
      } catch (NumberFormatException var18) {
         System.err.printf("Invalid threshold argument: %s%n", new Object[]{var18.getMessage()});
         return 1;
      }

      if(sizeThreshold >= 0L && cellCountThreshold >= 0 && tombstoneCountThreshold >= 0 && currentTime >= 0L) {
         List<File> directories = new ArrayList();
         List<SSTablePartitions.ExtendedDescriptor> descriptors = new ArrayList();
         if(!argumentsToFiles(cmd.getArgs(), descriptors, directories)) {
            return 1;
         } else {
            Iterator var16 = directories.iterator();

            while(var16.hasNext()) {
               File directory = (File)var16.next();
               processDirectory(scanRecursive, withSnapshots, withBackups, directory, descriptors);
            }

            if(csv) {
               System.out.println("key,keyBinary,live,offset,size,rowCount,cellCount,tombstoneCount,rowTombstoneCount,rangeTombstoneCount,complexTombstoneCount,cellTombstoneCount,rowTtlExpired,cellTtlExpired,directory,keyspace,table,index,snapshot,backup,generation,format,version");
            }

            Collections.sort(descriptors);
            var16 = descriptors.iterator();

            while(var16.hasNext()) {
               SSTablePartitions.ExtendedDescriptor desc = (SSTablePartitions.ExtendedDescriptor)var16.next();
               processSstable(keys, excludes, desc, sizeThreshold, cellCountThreshold, tombstoneCountThreshold, partitionsOnly, csv, currentTime);
            }

            return 0;
         }
      } else {
         System.err.println("Negative values are not allowed");
         return 1;
      }
   }

   private static void processDirectory(boolean scanRecursive, boolean withSnapshots, boolean withBackups, File dir, List<SSTablePartitions.ExtendedDescriptor> descriptors) {
      File[] files = dir.listFiles();
      if(files != null) {
         File[] var6 = files;
         int var7 = files.length;

         for(int var8 = 0; var8 < var7; ++var8) {
            File file = var6[var8];
            if(file.isFile()) {
               try {
                  if(Descriptor.componentFromFilename(file) != Component.DATA) {
                     continue;
                  }

                  SSTablePartitions.ExtendedDescriptor desc = SSTablePartitions.ExtendedDescriptor.guessFromFile(file);
                  if(desc.snapshot != null && !withSnapshots || desc.backup != null && !withBackups) {
                     continue;
                  }

                  descriptors.add(desc);
               } catch (IllegalArgumentException var11) {
                  ;
               }
            }

            if(scanRecursive && file.isDirectory()) {
               processDirectory(true, withSnapshots, withBackups, file, descriptors);
            }
         }

      }
   }

   private static boolean argumentsToFiles(String[] args, List<SSTablePartitions.ExtendedDescriptor> descriptors, List<File> directories) {
      boolean err = false;
      String[] var4 = args;
      int var5 = args.length;

      for(int var6 = 0; var6 < var5; ++var6) {
         String arg = var4[var6];
         File fArg = new File(arg);
         if(!fArg.exists()) {
            System.err.printf("Argument '%s' does not resolve to a file or directory%n", new Object[]{arg});
            err = true;
         }

         if(!fArg.canRead()) {
            System.err.printf("Argument '%s' is not a readable file or directory (check permissions)%n", new Object[]{arg});
            err = true;
         } else {
            if(fArg.isFile()) {
               try {
                  descriptors.add(SSTablePartitions.ExtendedDescriptor.guessFromFile(fArg));
               } catch (IllegalArgumentException var10) {
                  System.err.printf("Argument '%s' is not an sstable%n", new Object[]{arg});
                  err = true;
               }
            }

            if(fArg.isDirectory()) {
               directories.add(fArg);
            }
         }
      }

      return !err;
   }

   private static void processSstable(String[] keys, HashSet<String> excludes, SSTablePartitions.ExtendedDescriptor desc, long sizeThreshold, int cellCountThreshold, int tombstoneCountThreshold, boolean partitionsOnly, boolean csv, long currentTime) {
      try {
         long t0 = System.nanoTime();
         TableMetadata metadata = Util.metadataFromSSTable(desc.descriptor, desc.keyspace, desc.table);
         SSTableReader sstable = SSTableReader.openNoValidation(desc.descriptor, TableMetadataRef.forOfflineTools(metadata));
         if(!csv) {
            System.out.printf("%nProcessing %s (%d bytes uncompressed, %d bytes on disk)%n", new Object[]{desc, Long.valueOf(sstable.uncompressedLength()), Long.valueOf(sstable.onDiskLength())});
         }

         List<SSTablePartitions.PartitionStatistics> matches = new ArrayList();
         SSTablePartitions.SstableStatistics sstableStatistics = new SSTablePartitions.SstableStatistics();

         try {
            IPartitioner partitioner = sstable.getPartitioner();
            ISSTableScanner currentScanner = null;
            if(keys != null && keys.length > 0) {
               try {
                  Stream var10000 = Arrays.stream(keys).filter((key) -> {
                     return !excludes.contains(key);
                  });
                  AbstractType var10001 = metadata.partitionKeyType;
                  metadata.partitionKeyType.getClass();
                  var10000 = var10000.map(var10001::fromString);
                  partitioner.getClass();
                  List<AbstractBounds<PartitionPosition>> bounds = (List)var10000.map(partitioner::decorateKey).sorted().map(PartitionPosition::getToken).map((token) -> {
                     return new Bounds(token.minKeyBound(), token.maxKeyBound());
                  }).collect(Collectors.toList());
                  currentScanner = sstable.getScanner(bounds.iterator());
               } catch (RuntimeException var33) {
                  System.err.printf("Cannot use one or more partition keys in %s for the partition key type ('%s') of the underlying table: %s%n", new Object[]{Arrays.toString(keys), metadata.partitionKeyType.asCQL3Type(), var33});
               }
            }

            if(currentScanner == null) {
               currentScanner = sstable.getScanner();
            }

            try {
               SSTablePartitions.PartitionStatistics statistics = null;

               while(currentScanner.hasNext()) {
                  UnfilteredRowIterator partition = (UnfilteredRowIterator)currentScanner.next();
                  checkMatch(sizeThreshold, cellCountThreshold, tombstoneCountThreshold, csv, partitionsOnly, currentScanner, matches, sstableStatistics, statistics, metadata, desc);
                  statistics = new SSTablePartitions.PartitionStatistics(partition.partitionKey().getKey(), currentScanner.getCurrentPosition(), partition.partitionLevelDeletion().isLive());
                  if(!excludes.contains(metadata.partitionKeyType.getString(statistics.key)) && !partitionsOnly) {
                     perPartitionDetails(desc, currentTime, statistics, partition);
                  }
               }

               checkMatch(sizeThreshold, cellCountThreshold, tombstoneCountThreshold, csv, partitionsOnly, currentScanner, matches, sstableStatistics, statistics, metadata, desc);
            } finally {
               currentScanner.close();
            }
         } catch (RuntimeException var35) {
            System.err.printf("Failure processing sstable %s: %s%n", new Object[]{desc.descriptor, var35});
         } finally {
            sstable.selfRef().release();
         }

         long t = System.nanoTime() - t0;
         if(!csv) {
            if(!matches.isEmpty()) {
               System.out.printf("Summary of %s:%n  File: %s%n  %d partitions match%n  Keys:", new Object[]{desc, desc.descriptor.filenameFor(Component.DATA), Integer.valueOf(matches.size())});
               Iterator var40 = matches.iterator();

               while(var40.hasNext()) {
                  SSTablePartitions.PartitionStatistics match = (SSTablePartitions.PartitionStatistics)var40.next();
                  System.out.print(" " + maybeEscapeKeyForSummary(metadata, match.key));
               }

               System.out.println();
            }

            if(partitionsOnly) {
               System.out.printf("        %20s%n", new Object[]{"Partition size"});
            } else {
               System.out.printf("        %20s %20s %20s %20s%n", new Object[]{"Partition size", "Row count", "Cell count", "Tombstone count"});
            }

            printPercentile(partitionsOnly, sstableStatistics, "p50", (h) -> {
               return Long.valueOf(h.percentile(0.5D));
            });
            printPercentile(partitionsOnly, sstableStatistics, "p75", (h) -> {
               return Long.valueOf(h.percentile(0.75D));
            });
            printPercentile(partitionsOnly, sstableStatistics, "p90", (h) -> {
               return Long.valueOf(h.percentile(0.9D));
            });
            printPercentile(partitionsOnly, sstableStatistics, "p95", (h) -> {
               return Long.valueOf(h.percentile(0.95D));
            });
            printPercentile(partitionsOnly, sstableStatistics, "p99", (h) -> {
               return Long.valueOf(h.percentile(0.99D));
            });
            printPercentile(partitionsOnly, sstableStatistics, "p999", (h) -> {
               return Long.valueOf(h.percentile(0.999D));
            });
            printPercentile(partitionsOnly, sstableStatistics, "min", EstimatedHistogram::min);
            printPercentile(partitionsOnly, sstableStatistics, "max", EstimatedHistogram::max);
            System.out.printf("  count %20d%n", new Object[]{Long.valueOf(sstableStatistics.partitionSizeHistogram.count())});
            System.out.printf("  time  %20d%n", new Object[]{Long.valueOf(TimeUnit.NANOSECONDS.toMillis(t))});
         }
      } catch (IOException var37) {
         var37.printStackTrace(System.err);
      }

   }

   private static void perPartitionDetails(SSTablePartitions.ExtendedDescriptor desc, long currentTime, SSTablePartitions.PartitionStatistics statistics, UnfilteredRowIterator partition) {
      label55:
      while(partition.hasNext()) {
         Unfiltered unfiltered = (Unfiltered)partition.next();
         if(!(unfiltered instanceof Row)) {
            if(!(unfiltered instanceof RangeTombstoneMarker)) {
               throw new UnsupportedOperationException("Unknown kind " + unfiltered.kind() + " in sstable " + desc.descriptor);
            }

            ++statistics.rangeTombstoneCount;
         } else {
            Row row = (Row)unfiltered;
            ++statistics.rowCount;
            if(!row.deletion().isLive()) {
               ++statistics.rowTombstoneCount;
            }

            LivenessInfo liveInfo = row.primaryKeyLivenessInfo();
            if(!liveInfo.isEmpty() && liveInfo.isExpiring() && (long)liveInfo.localExpirationTime() < currentTime) {
               ++statistics.rowTtlExpired;
            }

            Iterator var8 = row.iterator();

            while(true) {
               while(true) {
                  if(!var8.hasNext()) {
                     continue label55;
                  }

                  ColumnData cd = (ColumnData)var8.next();
                  if(cd.column().isSimple()) {
                     cellStats((int)currentTime, statistics, liveInfo, (Cell)cd);
                  } else {
                     ComplexColumnData complexData = (ComplexColumnData)cd;
                     if(!complexData.complexDeletion().isLive()) {
                        ++statistics.complexTombstoneCount;
                     }

                     Iterator var11 = complexData.iterator();

                     while(var11.hasNext()) {
                        Cell cell = (Cell)var11.next();
                        cellStats((int)currentTime, statistics, liveInfo, cell);
                     }
                  }
               }
            }
         }
      }

   }

   private static void cellStats(int currentTime, SSTablePartitions.PartitionStatistics statistics, LivenessInfo liveInfo, Cell cell) {
      ++statistics.cellCount;
      if(cell.isTombstone()) {
         ++statistics.cellTombstoneCount;
      }

      if(cell.isExpiring() && (liveInfo.isEmpty() || cell.ttl() != liveInfo.ttl()) && !cell.isLive(currentTime)) {
         ++statistics.cellTtlExpired;
      }

   }

   private static void printPercentile(boolean partitionsOnly, SSTablePartitions.SstableStatistics sstableStatistics, String header, Function<EstimatedHistogram, Long> value) {
      if(partitionsOnly) {
         System.out.printf("  %-4s  %20d%n", new Object[]{header, value.apply(sstableStatistics.partitionSizeHistogram)});
      } else {
         System.out.printf("  %-4s  %20d %20d %20d %20d%n", new Object[]{header, value.apply(sstableStatistics.partitionSizeHistogram), value.apply(sstableStatistics.rowCountHistogram), value.apply(sstableStatistics.cellCountHistogram), value.apply(sstableStatistics.tombstoneCountHistogram)});
      }

   }

   private static String maybeEscapeKeyForSummary(TableMetadata metadata, ByteBuffer key) {
      String s = metadata.partitionKeyType.getString(key);
      return s.indexOf(32) == -1?s:"\"" + s.replaceAll("\"", "\"\"") + "\"";
   }

   private static void checkMatch(long sizeThreshold, int cellCountThreshold, int tombstoneCountThreshold, boolean csv, boolean partitionsOnly, ISSTableScanner currentScanner, List<SSTablePartitions.PartitionStatistics> matches, SSTablePartitions.SstableStatistics sstableStatistics, SSTablePartitions.PartitionStatistics statistics, TableMetadata metadata, SSTablePartitions.ExtendedDescriptor desc) {
      if(statistics != null) {
         statistics.endOfPartition(currentScanner.getCurrentPosition());
         sstableStatistics.partitionSizeHistogram.add(statistics.partitionSize);
         sstableStatistics.rowCountHistogram.add((long)statistics.rowCount);
         sstableStatistics.cellCountHistogram.add((long)statistics.cellCount);
         sstableStatistics.tombstoneCountHistogram.add((long)statistics.tombstoneCount());
         if(statistics.matchesThreshold(sizeThreshold, cellCountThreshold, tombstoneCountThreshold)) {
            matches.add(statistics);
            if(csv) {
               statistics.printPartitionInfoCSV(metadata, desc);
            } else {
               statistics.printPartitionInfo(metadata, partitionsOnly);
            }
         }
      }

   }

   private static void printUsage() {
      String usage = String.format("sstablepartitions <options> <sstable files or directories>%n", new Object[0]);
      String header = "Print partition statistics of one or more sstables.";
      (new HelpFormatter()).printHelp(usage, header, options, "");
   }

   static String notNull(String s) {
      return s != null?s:"";
   }

   static TableId notNull(TableId s) {
      return s != null?s:EMPTY_TABLE_ID;
   }

   static {
      DatabaseDescriptor.clientInitialization(true, true, new Config());
      Option optKey = new Option("k", "key", true, "Partition keys to include");
      optKey.setArgs(-2);
      options.addOption(optKey);
      Option excludeKey = new Option("x", "exclude-key", true, "Excluded partition key(s) from partition detailed row/cell/tombstone information (irrelevant, if --partitions-only is given)");
      excludeKey.setArgs(-2);
      options.addOption(excludeKey);
      Option thresholdKey = new Option("t", "min-size", true, "partition size threshold");
      options.addOption(thresholdKey);
      Option tombstoneKey = new Option("o", "min-tombstones", true, "partition tombstone count threshold");
      options.addOption(tombstoneKey);
      Option cellKey = new Option("c", "min-cells", true, "partition cell count threshold");
      options.addOption(cellKey);
      Option currentTimestampKey = new Option("u", "current-timestamp", true, "timestamp (seconds since epoch, unit time) for TTL expired calculation");
      options.addOption(currentTimestampKey);
      Option recursiveKey = new Option("r", "recursive", false, "scan for sstables recursively");
      options.addOption(recursiveKey);
      Option snapshotsKey = new Option("s", "snapshots", false, "include snapshots present in data directories (recursive scans)");
      options.addOption(snapshotsKey);
      Option backupsKey = new Option("b", "backups", false, "include backups present in data directories (recursive scans)");
      options.addOption(backupsKey);
      Option partitionsOnlyKey = new Option("y", "partitions-only", false, "Do not process per-partition detailed row/cell/tombstone information, only brief information");
      options.addOption(partitionsOnlyKey);
      Option csvKey = new Option("m", "csv", false, "CSV output (machine readable)");
      options.addOption(csvKey);
   }

   static final class ExtendedDescriptor implements Comparable<SSTablePartitions.ExtendedDescriptor> {
      final String keyspace;
      final String table;
      final String index;
      final String snapshot;
      final String backup;
      final TableId tableId;
      final Descriptor descriptor;

      ExtendedDescriptor(String keyspace, String table, TableId tableId, String index, String snapshot, String backup, Descriptor descriptor) {
         this.keyspace = keyspace;
         this.table = table;
         this.tableId = tableId;
         this.index = index;
         this.snapshot = snapshot;
         this.backup = backup;
         this.descriptor = descriptor;
      }

      public String toString() {
         StringBuilder sb = new StringBuilder();
         if(this.backup != null) {
            sb.append("Backup:").append(this.backup).append(' ');
         }

         if(this.snapshot != null) {
            sb.append("Snapshot:").append(this.snapshot).append(' ');
         }

         if(this.keyspace != null) {
            sb.append(this.keyspace).append('.');
         }

         if(this.table != null) {
            sb.append(this.table);
         }

         if(this.index != null) {
            sb.append('.').append(this.index);
         }

         if(this.tableId != null) {
            sb.append('-').append(this.tableId.toHexString());
         }

         sb.append(" #").append(this.descriptor.generation).append(" (").append(this.descriptor.formatType.name).append('-').append(this.descriptor.version.getVersion()).append(')');
         return sb.toString();
      }

      static SSTablePartitions.ExtendedDescriptor guessFromFile(File fArg) {
         Descriptor desc = Descriptor.fromFilename(fArg);
         String snapshot = null;
         String backup = null;
         String index = null;
         File parent = fArg.getParentFile();
         File parent2 = parent.getParentFile();
         String parentName = parent.getName();
         String parent2Name = parent2.getName();
         if(parentName.length() > 1 && parentName.startsWith(".") && parentName.charAt(1) != 46) {
            index = parentName.substring(1);
            parent = parent.getParentFile();
            parent2 = parent.getParentFile();
            parentName = parent.getName();
         }

         if(parent2Name.equals("snapshots")) {
            snapshot = parent.getName();
            parent = parent2.getParentFile();
            parent2 = parent.getParentFile();
            parentName = parent.getName();
            parent2Name = parent2.getName();
         } else if(parent2Name.equals("backups")) {
            backup = parent.getName();
            parent = parent2.getParentFile();
            parent2 = parent.getParentFile();
            parentName = parent.getName();
            parent2Name = parent2.getName();
         }

         try {
            Pair<String, TableId> tableNameAndId = TableId.tableNameAndIdFromFilename(parentName);
            if(tableNameAndId != null) {
               return new SSTablePartitions.ExtendedDescriptor(parent2Name, (String)tableNameAndId.left, (TableId)tableNameAndId.right, index, snapshot, backup, desc);
            }
         } catch (NumberFormatException var11) {
            ;
         }

         return new SSTablePartitions.ExtendedDescriptor((String)null, (String)null, (TableId)null, index, snapshot, backup, desc);
      }

      public int compareTo(SSTablePartitions.ExtendedDescriptor o) {
         int c = this.descriptor.directory.toString().compareTo(o.descriptor.directory.toString());
         if(c != 0) {
            return c;
         } else {
            c = SSTablePartitions.notNull(this.keyspace).compareTo(SSTablePartitions.notNull(o.keyspace));
            if(c != 0) {
               return c;
            } else {
               c = SSTablePartitions.notNull(this.table).compareTo(SSTablePartitions.notNull(o.table));
               if(c != 0) {
                  return c;
               } else {
                  c = SSTablePartitions.notNull(this.tableId).compareTo(SSTablePartitions.notNull(o.tableId));
                  if(c != 0) {
                     return c;
                  } else {
                     c = SSTablePartitions.notNull(this.index).compareTo(SSTablePartitions.notNull(o.index));
                     if(c != 0) {
                        return c;
                     } else {
                        c = SSTablePartitions.notNull(this.snapshot).compareTo(SSTablePartitions.notNull(o.snapshot));
                        if(c != 0) {
                           return c;
                        } else {
                           c = SSTablePartitions.notNull(this.backup).compareTo(SSTablePartitions.notNull(o.backup));
                           if(c != 0) {
                              return c;
                           } else {
                              c = Integer.compare(this.descriptor.generation, o.descriptor.generation);
                              return c != 0?c:Integer.compare(System.identityHashCode(this), System.identityHashCode(o));
                           }
                        }
                     }
                  }
               }
            }
         }
      }
   }

   static final class PartitionStatistics {
      final ByteBuffer key;
      final long offset;
      final boolean live;
      long partitionSize;
      int rowCount;
      int cellCount;
      int rowTombstoneCount;
      int rangeTombstoneCount;
      int complexTombstoneCount;
      int cellTombstoneCount;
      int rowTtlExpired;
      int cellTtlExpired;

      PartitionStatistics(ByteBuffer key, long offset, boolean live) {
         this.key = key;
         this.offset = offset;
         this.partitionSize = -1L;
         this.live = live;
         this.rowCount = 0;
         this.cellCount = 0;
         this.rowTombstoneCount = 0;
         this.rangeTombstoneCount = 0;
         this.complexTombstoneCount = 0;
         this.cellTombstoneCount = 0;
         this.rowTtlExpired = 0;
         this.cellTtlExpired = 0;
      }

      void endOfPartition(long position) {
         this.partitionSize = position - this.offset;
      }

      int tombstoneCount() {
         return this.rowTombstoneCount + this.rangeTombstoneCount + this.complexTombstoneCount + this.cellTombstoneCount + this.rowTtlExpired + this.cellTtlExpired;
      }

      boolean matchesThreshold(long sizeThreshold, int cellCountThreshold, int tombstoneCountThreshold) {
         return this.partitionSize >= sizeThreshold || this.cellCount >= cellCountThreshold || this.tombstoneCount() >= tombstoneCountThreshold;
      }

      void printPartitionInfo(TableMetadata metadata, boolean partitionsOnly) {
         String key = metadata.partitionKeyType.getString(this.key);
         if(partitionsOnly) {
            System.out.printf("  Partition: '%s' (%s) %s, position: %d, size: %d%n", new Object[]{key, ByteBufferUtil.bytesToHex(this.key), this.live?"live":"not live", Long.valueOf(this.offset), Long.valueOf(this.partitionSize)});
         } else {
            System.out.printf("  Partition: '%s' (%s) %s, position: %d, size: %d, rows: %d, cells: %d, tombstones: %d (row:%d, range:%d, complex:%d, cell:%d, row-TTLd:%d, cell-TTLd:%d)%n", new Object[]{key, ByteBufferUtil.bytesToHex(this.key), this.live?"live":"not live", Long.valueOf(this.offset), Long.valueOf(this.partitionSize), Integer.valueOf(this.rowCount), Integer.valueOf(this.cellCount), Integer.valueOf(this.tombstoneCount()), Integer.valueOf(this.rowTombstoneCount), Integer.valueOf(this.rangeTombstoneCount), Integer.valueOf(this.complexTombstoneCount), Integer.valueOf(this.cellTombstoneCount), Integer.valueOf(this.rowTtlExpired), Integer.valueOf(this.cellTtlExpired)});
         }

      }

      void printPartitionInfoCSV(TableMetadata metadata, SSTablePartitions.ExtendedDescriptor desc) {
         System.out.printf("\"%s\",%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s,%s,%s,%s,%s,%s,%d,%s,%s%n", new Object[]{SSTablePartitions.maybeEscapeKeyForSummary(metadata, this.key), ByteBufferUtil.bytesToHex(this.key), this.live?"true":"false", Long.valueOf(this.offset), Long.valueOf(this.partitionSize), Integer.valueOf(this.rowCount), Integer.valueOf(this.cellCount), Integer.valueOf(this.tombstoneCount()), Integer.valueOf(this.rowTombstoneCount), Integer.valueOf(this.rangeTombstoneCount), Integer.valueOf(this.complexTombstoneCount), Integer.valueOf(this.cellTombstoneCount), Integer.valueOf(this.rowTtlExpired), Integer.valueOf(this.cellTtlExpired), desc.descriptor.filenameFor(Component.DATA), SSTablePartitions.notNull(desc.keyspace), SSTablePartitions.notNull(desc.table), SSTablePartitions.notNull(desc.index), SSTablePartitions.notNull(desc.snapshot), SSTablePartitions.notNull(desc.backup), Integer.valueOf(desc.descriptor.generation), desc.descriptor.formatType.name, desc.descriptor.version.getVersion()});
      }
   }

   static final class SstableStatistics {
      EstimatedHistogram partitionSizeHistogram = new EstimatedHistogram();
      EstimatedHistogram rowCountHistogram = new EstimatedHistogram();
      EstimatedHistogram cellCountHistogram = new EstimatedHistogram();
      EstimatedHistogram tombstoneCountHistogram = new EstimatedHistogram();

      SstableStatistics() {
      }
   }
}
