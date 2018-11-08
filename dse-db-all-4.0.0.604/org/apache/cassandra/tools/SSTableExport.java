package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class SSTableExport {
   private static final String KEY_OPTION = "k";
   private static final String DEBUG_OUTPUT_OPTION = "d";
   private static final String EXCLUDE_KEY_OPTION = "x";
   private static final String ENUMERATE_KEYS_OPTION = "e";
   private static final String RAW_TIMESTAMPS = "t";
   private static final String PARTITION_JSON_LINES = "l";
   private static final Options options = new Options();
   private static CommandLine cmd;

   public SSTableExport() {
   }

   public static void main(String[] args) throws ConfigurationException {
      PosixParser parser = new PosixParser();

      try {
         cmd = parser.parse(options, args);
      } catch (ParseException var20) {
         System.err.println(var20.getMessage());
         printUsage();
         System.exit(1);
      }

      if(cmd.getArgs().length != 1) {
         System.err.println("You must supply exactly one sstable");
         printUsage();
         System.exit(1);
      }

      String[] keys = cmd.getOptionValues("k");
      HashSet<String> excludes = new HashSet(Arrays.asList(cmd.getOptionValues("x") == null?new String[0]:cmd.getOptionValues("x")));
      String ssTableFileName = (new File(cmd.getArgs()[0])).getAbsolutePath();
      if(!(new File(ssTableFileName)).exists()) {
         System.err.println("Cannot find file " + ssTableFileName);
         System.exit(1);
      }

      Descriptor desc = Descriptor.fromFilename(ssTableFileName);

      try {
         TableMetadata metadata = Util.metadataFromSSTable(desc);
         if(cmd.hasOption("e")) {
            PartitionIndexIterator iter = desc.getFormat().getReaderFactory().keyIterator(desc, metadata);
            Throwable var8 = null;

            try {
               JsonTransformer.keysToJson((ISSTableScanner)null, Util.iterToStream(iter), cmd.hasOption("t"), metadata, System.out);
            } catch (Throwable var19) {
               var8 = var19;
               throw var19;
            } finally {
               if(iter != null) {
                  if(var8 != null) {
                     try {
                        iter.close();
                     } catch (Throwable var18) {
                        var8.addSuppressed(var18);
                     }
                  } else {
                     iter.close();
                  }
               }

            }
         } else {
            SSTableReader sstable = SSTableReader.openNoValidation(desc, TableMetadataRef.forOfflineTools(metadata));
            IPartitioner partitioner = sstable.getPartitioner();
            ISSTableScanner currentScanner;
            if(keys != null && keys.length > 0) {
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
            } else {
               currentScanner = sstable.getScanner();
            }

            Stream<UnfilteredRowIterator> partitions = Util.iterToStream((Iterator)currentScanner).filter((i) -> {
               return excludes.isEmpty() || !excludes.contains(metadata.partitionKeyType.getString(i.partitionKey().getKey()));
            });
            if(cmd.hasOption("d")) {
               AtomicLong position = new AtomicLong();
               partitions.forEach((partition) -> {
                  position.set(currentScanner.getCurrentPosition());
                  if(!partition.partitionLevelDeletion().isLive()) {
                     System.out.println("[" + metadata.partitionKeyType.getString(partition.partitionKey().getKey()) + "]@" + position.get() + " " + partition.partitionLevelDeletion());
                  }

                  if(!partition.staticRow().isEmpty()) {
                     System.out.println("[" + metadata.partitionKeyType.getString(partition.partitionKey().getKey()) + "]@" + position.get() + " " + partition.staticRow().toString(metadata, true));
                  }

                  partition.forEachRemaining((row) -> {
                     System.out.println("[" + metadata.partitionKeyType.getString(partition.partitionKey().getKey()) + "]@" + position.get() + " " + row.toString(metadata, false, true));
                     position.set(currentScanner.getCurrentPosition());
                  });
               });
            } else if(cmd.hasOption("l")) {
               JsonTransformer.toJsonLines(currentScanner, partitions, cmd.hasOption("t"), metadata, System.out);
            } else {
               JsonTransformer.toJson(currentScanner, partitions, cmd.hasOption("t"), metadata, System.out);
            }
         }
      } catch (IOException var22) {
         var22.printStackTrace(System.err);
      }

      System.exit(0);
   }

   private static void printUsage() {
      String usage = String.format("sstabledump <sstable file path> <options>%n", new Object[0]);
      String header = "Dump contents of given SSTable to standard output in JSON format.";
      (new HelpFormatter()).printHelp(usage, header, options, "");
   }

   static {
      DatabaseDescriptor.clientInitialization();
      TPC.ensureInitialized(true);
      Option optKey = new Option("k", true, "Partition key");
      optKey.setArgs(-2);
      options.addOption(optKey);
      Option excludeKey = new Option("x", true, "Excluded partition key");
      excludeKey.setArgs(-2);
      options.addOption(excludeKey);
      Option optEnumerate = new Option("e", false, "enumerate partition keys only");
      options.addOption(optEnumerate);
      Option debugOutput = new Option("d", false, "CQL row per line internal representation");
      options.addOption(debugOutput);
      Option rawTimestamps = new Option("t", false, "Print raw timestamps instead of iso8601 date strings");
      options.addOption(rawTimestamps);
      Option partitionJsonLines = new Option("l", false, "Output json lines, by partition");
      options.addOption(partitionJsonLines);
   }
}
