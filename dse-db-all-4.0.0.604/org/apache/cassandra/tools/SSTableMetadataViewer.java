package org.apache.cassandra.tools;

import com.google.common.collect.MinMaxPriorityQueue;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.joda.time.Duration;
import org.joda.time.format.PeriodFormat;

public class SSTableMetadataViewer {
   private static final Options options = new Options();
   private static CommandLine cmd;
   private static final String COLORS = "c";
   private static final String UNICODE = "u";
   private static final String GCGS_KEY = "g";
   private static final String TIMESTAMP_UNIT = "t";
   private static final String SCAN = "s";
   private static Comparator<SSTableMetadataViewer.ValuedByteBuffer> VCOMP = Comparator.comparingLong(SSTableMetadataViewer.ValuedByteBuffer::getValue).reversed();
   boolean color;
   boolean unicode;
   int gc;
   PrintStream out;
   String[] files;
   TimeUnit tsUnit;

   public SSTableMetadataViewer() {
      this(true, true, 0, TimeUnit.MICROSECONDS, System.out);
   }

   public SSTableMetadataViewer(boolean color, boolean unicode, int gc, TimeUnit tsUnit, PrintStream out) {
      this.color = color;
      this.tsUnit = tsUnit;
      this.unicode = unicode;
      this.out = out;
      this.gc = gc;
   }

   public static String deletion(long time) {
      return time != 0L && time != 2147483647L?toDateString(time, TimeUnit.SECONDS):"no tombstones";
   }

   public static String toDateString(long time, TimeUnit unit) {
      return time == 0L?null:(new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")).format(new Date(unit.toMillis(time)));
   }

   public static String toDurationString(long duration, TimeUnit unit) {
      return duration == 0L?null:(duration == 2147483647L?"never":PeriodFormat.getDefault().print((new Duration(unit.toMillis(duration))).toPeriod()));
   }

   public static String toByteString(long bytes) {
      if(bytes == 0L) {
         return null;
      } else if(bytes < 1024L) {
         return bytes + " B";
      } else {
         int exp = (int)(Math.log((double)bytes) / Math.log(1024.0D));
         char pre = "kMGTP".charAt(exp - 1);
         return String.format("%.1f %sB", new Object[]{Double.valueOf((double)bytes / Math.pow(1024.0D, (double)exp)), Character.valueOf(pre)});
      }
   }

   public String scannedOverviewOutput(String key, long value) {
      StringBuilder sb = new StringBuilder();
      if(this.color) {
         sb.append("\u001b[36m");
      }

      sb.append('[');
      if(this.color) {
         sb.append("\u001b[0m");
      }

      sb.append(key);
      if(this.color) {
         sb.append("\u001b[36m");
      }

      sb.append("] ");
      if(this.color) {
         sb.append("\u001b[0m");
      }

      sb.append(value);
      return sb.toString();
   }

   private void printScannedOverview(Descriptor descriptor, StatsMetadata stats) throws IOException {
      TableMetadata cfm = Util.metadataFromSSTable(descriptor);
      SSTableReader reader = SSTableReader.openNoValidation(descriptor, TableMetadataRef.forOfflineTools(cfm));

      try {
         ISSTableScanner scanner = reader.getScanner();
         Throwable var6 = null;

         try {
            long bytes = scanner.getLengthInBytes();
            MinMaxPriorityQueue<SSTableMetadataViewer.ValuedByteBuffer> widestPartitions = MinMaxPriorityQueue.orderedBy(VCOMP).maximumSize(5).create();
            MinMaxPriorityQueue<SSTableMetadataViewer.ValuedByteBuffer> largestPartitions = MinMaxPriorityQueue.orderedBy(VCOMP).maximumSize(5).create();
            MinMaxPriorityQueue<SSTableMetadataViewer.ValuedByteBuffer> mostTombstones = MinMaxPriorityQueue.orderedBy(VCOMP).maximumSize(5).create();
            long partitionCount = 0L;
            long rowCount = 0L;
            long tombstoneCount = 0L;
            long cellCount = 0L;
            double totalCells = (double)stats.totalColumnsSet;
            int lastPercent = 0;
            long lastPercentTime = 0L;

            label483:
            while(true) {
               if(scanner.hasNext()) {
                  UnfilteredRowIterator partition = (UnfilteredRowIterator)scanner.next();
                  Throwable var77 = null;

                  try {
                     long psize = 0L;
                     long pcount = 0L;
                     int ptombcount = 0;
                     ++partitionCount;
                     if(!partition.staticRow().isEmpty()) {
                        ++rowCount;
                        ++pcount;
                        psize += (long)partition.staticRow().dataSize();
                     }

                     if(!partition.partitionLevelDeletion().isLive()) {
                        ++tombstoneCount;
                        ++ptombcount;
                     }

                     while(true) {
                        label474:
                        while(partition.hasNext()) {
                           Unfiltered unfiltered = (Unfiltered)partition.next();
                           switch(null.$SwitchMap$org$apache$cassandra$db$rows$Unfiltered$Kind[unfiltered.kind().ordinal()]) {
                           case 1:
                              ++rowCount;
                              Row row = (Row)unfiltered;
                              psize += (long)row.dataSize();
                              ++pcount;
                              Iterator var34 = row.cells().iterator();

                              while(true) {
                                 if(!var34.hasNext()) {
                                    continue label474;
                                 }

                                 Cell cell = (Cell)var34.next();
                                 ++cellCount;
                                 double percentComplete = Math.min(1.0D, (double)cellCount / totalCells);
                                 if(lastPercent != (int)(percentComplete * 100.0D) && System.currentTimeMillis() - lastPercentTime > 1000L) {
                                    lastPercentTime = System.currentTimeMillis();
                                    lastPercent = (int)(percentComplete * 100.0D);
                                    if(this.color) {
                                       this.out.printf("\r%sAnalyzing SSTable...  %s%s %s(%%%s)", new Object[]{"\u001b[34m", "\u001b[36m", Util.progress(percentComplete, 30, this.unicode), "\u001b[0m", Integer.valueOf((int)(percentComplete * 100.0D))});
                                    } else {
                                       this.out.printf("\rAnalyzing SSTable...  %s (%%%s)", new Object[]{Util.progress(percentComplete, 30, this.unicode), Integer.valueOf((int)(percentComplete * 100.0D))});
                                    }

                                    this.out.flush();
                                 }

                                 if(cell.isTombstone()) {
                                    ++tombstoneCount;
                                    ++ptombcount;
                                 }
                              }
                           case 2:
                              ++tombstoneCount;
                              ++ptombcount;
                           }
                        }

                        widestPartitions.add(new SSTableMetadataViewer.ValuedByteBuffer(partition.partitionKey().getKey(), pcount));
                        largestPartitions.add(new SSTableMetadataViewer.ValuedByteBuffer(partition.partitionKey().getKey(), psize));
                        mostTombstones.add(new SSTableMetadataViewer.ValuedByteBuffer(partition.partitionKey().getKey(), (long)ptombcount));
                        continue label483;
                     }
                  } catch (Throwable var71) {
                     var77 = var71;
                     throw var71;
                  } finally {
                     if(partition != null) {
                        if(var77 != null) {
                           try {
                              partition.close();
                           } catch (Throwable var70) {
                              var77.addSuppressed(var70);
                           }
                        } else {
                           partition.close();
                        }
                     }

                  }
               }

               this.out.printf("\r%80s\r", new Object[]{" "});
               this.field("Size", Long.valueOf(bytes));
               this.field("Partitions", Long.valueOf(partitionCount));
               this.field("Rows", Long.valueOf(rowCount));
               this.field("Tombstones", Long.valueOf(tombstoneCount));
               this.field("Cells", Long.valueOf(cellCount));
               this.field("Widest Partitions", "");
               Util.iterToStream(widestPartitions.iterator()).sorted(VCOMP).forEach((p) -> {
                  this.out.println("  " + this.scannedOverviewOutput(cfm.partitionKeyType.getString(p.buffer), p.value));
               });
               this.field("Largest Partitions", "");
               Util.iterToStream(largestPartitions.iterator()).sorted(VCOMP).forEach((p) -> {
                  this.out.print("  ");
                  this.out.print(this.scannedOverviewOutput(cfm.partitionKeyType.getString(p.buffer), p.value));
                  if(this.color) {
                     this.out.print("\u001b[37m");
                  }

                  this.out.print(" (");
                  this.out.print(toByteString(p.value));
                  this.out.print(")");
                  if(this.color) {
                     this.out.print("\u001b[0m");
                  }

                  this.out.println();
               });
               StringBuilder tleaders = new StringBuilder();
               Util.iterToStream(mostTombstones.iterator()).sorted(VCOMP).forEach((p) -> {
                  if(p.value > 0L) {
                     tleaders.append("  ");
                     tleaders.append(this.scannedOverviewOutput(cfm.partitionKeyType.getString(p.buffer), p.value));
                     tleaders.append(System.lineSeparator());
                  }

               });
               String tombstoneLeaders = tleaders.toString();
               if(tombstoneLeaders.length() > 10) {
                  this.field("Tombstone Leaders", "");
                  this.out.print(tombstoneLeaders);
               }
               break;
            }
         } catch (Throwable var73) {
            var6 = var73;
            throw var73;
         } finally {
            if(scanner != null) {
               if(var6 != null) {
                  try {
                     scanner.close();
                  } catch (Throwable var69) {
                     var6.addSuppressed(var69);
                  }
               } else {
                  scanner.close();
               }
            }

         }
      } finally {
         reader.selfRef().ensureReleased();
      }

   }

   private void printSStableMetadata(String fname, boolean scan) throws IOException {
      Descriptor descriptor = Descriptor.fromFilename(fname);
      Map<MetadataType, MetadataComponent> metadata = descriptor.getMetadataSerializer().deserialize(descriptor, EnumSet.allOf(MetadataType.class));
      ValidationMetadata validation = (ValidationMetadata)metadata.get(MetadataType.VALIDATION);
      StatsMetadata stats = (StatsMetadata)metadata.get(MetadataType.STATS);
      CompactionMetadata compaction = (CompactionMetadata)metadata.get(MetadataType.COMPACTION);
      File compressionFile = new File(descriptor.filenameFor(Component.COMPRESSION_INFO));
      SerializationHeader.Component header = (SerializationHeader.Component)metadata.get(MetadataType.HEADER);
      CompressionMetadata compression = compressionFile.exists()?CompressionMetadata.create(fname):null;
      Throwable var11 = null;

      try {
         this.field("SSTable", descriptor);
         if(scan && descriptor.version.getVersion().compareTo("ma") >= 0) {
            this.printScannedOverview(descriptor, stats);
         }

         if(validation != null) {
            this.field("Partitioner", validation.partitioner);
            this.field("Bloom Filter FP chance", Double.valueOf(validation.bloomFilterFPChance));
         }

         List maxClusteringValues;
         if(stats != null) {
            this.field("Minimum timestamp", Long.valueOf(stats.minTimestamp), toDateString(stats.minTimestamp, this.tsUnit));
            this.field("Maximum timestamp", Long.valueOf(stats.maxTimestamp), toDateString(stats.maxTimestamp, this.tsUnit));
            this.field("SSTable min local deletion time", Integer.valueOf(stats.minLocalDeletionTime), deletion((long)stats.minLocalDeletionTime));
            this.field("SSTable max local deletion time", Integer.valueOf(stats.maxLocalDeletionTime), deletion((long)stats.maxLocalDeletionTime));
            this.field("Compressor", compression != null?compression.compressor().getClass().getName():"-");
            if(compression != null) {
               this.field("Compression ratio", Double.valueOf(stats.compressionRatio));
            }

            this.field("TTL min", Integer.valueOf(stats.minTTL), toDurationString((long)stats.minTTL, TimeUnit.SECONDS));
            this.field("TTL max", Integer.valueOf(stats.maxTTL), toDurationString((long)stats.maxTTL, TimeUnit.SECONDS));
            if(validation != null && header != null) {
               this.printMinMaxToken(descriptor, FBUtilities.newPartitioner(descriptor), header.getKeyType());
            }

            if(header != null && header.getClusteringTypes().size() == stats.minClusteringValues.size()) {
               List<AbstractType<?>> clusteringTypes = header.getClusteringTypes();
               List<ByteBuffer> minClusteringValues = stats.minClusteringValues;
               maxClusteringValues = stats.maxClusteringValues;
               String[] minValues = new String[clusteringTypes.size()];
               String[] maxValues = new String[clusteringTypes.size()];

               for(int i = 0; i < clusteringTypes.size(); ++i) {
                  minValues[i] = ((AbstractType)clusteringTypes.get(i)).getString((ByteBuffer)minClusteringValues.get(i));
                  maxValues[i] = ((AbstractType)clusteringTypes.get(i)).getString((ByteBuffer)maxClusteringValues.get(i));
               }

               this.field("minClusteringValues", Arrays.toString(minValues));
               this.field("maxClusteringValues", Arrays.toString(maxValues));
            }

            this.field("Estimated droppable tombstones", Double.valueOf(stats.getEstimatedDroppableTombstoneRatio((int)(System.currentTimeMillis() / 1000L) - this.gc)));
            this.field("SSTable Level", Integer.valueOf(stats.sstableLevel));
            this.field("Repaired at", Long.valueOf(stats.repairedAt), toDateString(stats.repairedAt, TimeUnit.MILLISECONDS));
            this.field("Pending repair", stats.pendingRepair);
            this.field("Replay positions covered", stats.commitLogIntervals);
            this.field("totalColumnsSet", Long.valueOf(stats.totalColumnsSet));
            this.field("totalRows", Long.valueOf(stats.totalRows));
            this.field("Estimated tombstone drop times", "");
            Util.TermHistogram estDropped = new Util.TermHistogram(stats.estimatedTombstoneDropTime, "Drop Time", (offset) -> {
               return String.format("%d %s", new Object[]{Long.valueOf(offset), Util.wrapQuiet(toDateString(offset, TimeUnit.SECONDS), this.color)});
            }, String::valueOf);
            estDropped.printHistogram(this.out, this.color, this.unicode);
            this.field("Partition Size", "");
            Util.TermHistogram rowSize = new Util.TermHistogram(stats.estimatedPartitionSize, "Size (bytes)", (offset) -> {
               return String.format("%d %s", new Object[]{Long.valueOf(offset), Util.wrapQuiet(toByteString(offset), this.color)});
            }, String::valueOf);
            rowSize.printHistogram(this.out, this.color, this.unicode);
            this.field("Column Count", "");
            Util.TermHistogram cellCount = new Util.TermHistogram(stats.estimatedColumnCount, "Columns", String::valueOf, String::valueOf);
            cellCount.printHistogram(this.out, this.color, this.unicode);
         }

         if(compaction != null) {
            this.field("Estimated cardinality", Long.valueOf(compaction.cardinalityEstimator.cardinality()));
         }

         if(header != null) {
            EncodingStats encodingStats = header.getEncodingStats();
            AbstractType<?> keyType = header.getKeyType();
            maxClusteringValues = header.getClusteringTypes();
            Map<ByteBuffer, AbstractType<?>> staticColumns = header.getStaticColumns();
            Map<String, String> statics = (Map)staticColumns.entrySet().stream().collect(Collectors.toMap((e) -> {
               return UTF8Type.instance.getString((ByteBuffer)e.getKey());
            }, (e) -> {
               return ((AbstractType)e.getValue()).toString();
            }));
            Map<ByteBuffer, AbstractType<?>> regularColumns = header.getRegularColumns();
            Map<String, String> regulars = (Map)regularColumns.entrySet().stream().collect(Collectors.toMap((e) -> {
               return UTF8Type.instance.getString((ByteBuffer)e.getKey());
            }, (e) -> {
               return ((AbstractType)e.getValue()).toString();
            }));
            this.field("EncodingStats minTTL", Integer.valueOf(encodingStats.minTTL), toDurationString((long)encodingStats.minTTL, TimeUnit.SECONDS));
            this.field("EncodingStats minLocalDeletionTime", Integer.valueOf(encodingStats.minLocalDeletionTime), toDateString((long)encodingStats.minLocalDeletionTime, TimeUnit.SECONDS));
            this.field("EncodingStats minTimestamp", Long.valueOf(encodingStats.minTimestamp), toDateString(encodingStats.minTimestamp, this.tsUnit));
            this.field("KeyType", keyType.toString());
            this.field("ClusteringTypes", maxClusteringValues.toString());
            this.field("StaticColumns", FBUtilities.toString(statics));
            this.field("RegularColumns", FBUtilities.toString(regulars));
         }
      } catch (Throwable var26) {
         var11 = var26;
         throw var26;
      } finally {
         if(compression != null) {
            if(var11 != null) {
               try {
                  compression.close();
               } catch (Throwable var25) {
                  var11.addSuppressed(var25);
               }
            } else {
               compression.close();
            }
         }

      }

   }

   private void field(String field, Object value) {
      this.field(field, value, (String)null);
   }

   private void field(String field, Object value, String comment) {
      StringBuilder sb = new StringBuilder();
      if(this.color) {
         sb.append("\u001b[34m");
      }

      sb.append(field);
      if(this.color) {
         sb.append("\u001b[36m");
      }

      sb.append(": ");
      if(this.color) {
         sb.append("\u001b[0m");
      }

      sb.append(value == null?"--":value.toString());
      if(comment != null) {
         if(this.color) {
            sb.append("\u001b[37m");
         }

         sb.append(" (");
         sb.append(comment);
         sb.append(")");
         if(this.color) {
            sb.append("\u001b[0m");
         }
      }

      this.out.println(sb.toString());
   }

   private static void printUsage() {
      PrintWriter errWriter = new PrintWriter(System.err, true);
      Throwable var1 = null;

      try {
         HelpFormatter formatter = new HelpFormatter();
         formatter.printHelp(errWriter, 120, "sstablemetadata <options> <sstable...>", String.format("%nDump information about SSTable[s] for Apache Cassandra 3.x%nOptions:", new Object[0]), options, 2, 1, "", true);
         errWriter.println();
      } catch (Throwable var10) {
         var1 = var10;
         throw var10;
      } finally {
         if(errWriter != null) {
            if(var1 != null) {
               try {
                  errWriter.close();
               } catch (Throwable var9) {
                  var1.addSuppressed(var9);
               }
            } else {
               errWriter.close();
            }
         }

      }

   }

   private void printMinMaxToken(Descriptor descriptor, IPartitioner partitioner, AbstractType<?> keyType) throws IOException {
      Pair<DecoratedKey, DecoratedKey> minMax = descriptor.getFormat().getReaderFactory().getKeyRange(descriptor, partitioner);
      if(minMax != null) {
         this.field("First token", ((DecoratedKey)minMax.left).getToken(), keyType.getString(((DecoratedKey)minMax.left).getKey()));
         this.field("Last token", ((DecoratedKey)minMax.right).getToken(), keyType.getString(((DecoratedKey)minMax.right).getKey()));
      }
   }

   public static void main(String[] args) throws IOException {
      CommandLineParser parser = new PosixParser();
      Option disableColors = new Option("c", "colors", false, "Use ANSI color sequences");
      disableColors.setOptionalArg(true);
      options.addOption(disableColors);
      Option unicode = new Option("u", "unicode", false, "Use unicode to draw histograms and progress bars");
      unicode.setOptionalArg(true);
      options.addOption(unicode);
      Option gcgs = new Option("g", "gc_grace_seconds", true, "Time to use when calculating droppable tombstones");
      gcgs.setOptionalArg(true);
      options.addOption(gcgs);
      Option tsUnit = new Option("t", "timestamp_unit", true, "Time unit that cell timestamps are written with");
      tsUnit.setOptionalArg(true);
      options.addOption(tsUnit);
      Option scanEnabled = new Option("s", "scan", false, "Full sstable scan for additional details. Only available in 3.0+ sstables. Defaults: false");
      scanEnabled.setOptionalArg(true);
      options.addOption(scanEnabled);

      try {
         cmd = parser.parse(options, args);
      } catch (ParseException var18) {
         System.err.println(var18.getMessage());
         printUsage();
         System.exit(1);
      }

      if(cmd.getArgs().length < 1) {
         System.err.println("You must supply at least one sstable");
         printUsage();
         System.exit(1);
      }

      boolean enabledColors = cmd.hasOption("c");
      boolean enabledUnicode = cmd.hasOption("u");
      boolean fullScan = cmd.hasOption("s");
      int gc = Integer.parseInt(cmd.getOptionValue("g", "0"));
      TimeUnit ts = TimeUnit.valueOf(cmd.getOptionValue("t", "MICROSECONDS"));
      SSTableMetadataViewer metawriter = new SSTableMetadataViewer(enabledColors, enabledUnicode, gc, ts, System.out);
      String[] var13 = cmd.getArgs();
      int var14 = var13.length;

      for(int var15 = 0; var15 < var14; ++var15) {
         String fname = var13[var15];
         File sstable = new File(fname);
         if(sstable.exists()) {
            metawriter.printSStableMetadata(sstable.getAbsolutePath(), fullScan);
         } else {
            System.out.println("No such file: " + fname);
         }
      }

   }

   static {
      DatabaseDescriptor.clientInitialization();
   }

   private static class ValuedByteBuffer {
      public long value;
      public ByteBuffer buffer;

      public ValuedByteBuffer(ByteBuffer buffer, long value) {
         this.value = value;
         this.buffer = buffer;
      }

      public long getValue() {
         return this.value;
      }
   }
}
