package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.PartitionPosition;
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
import org.apache.commons.cli.CommandLineParser;

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

   public static void main(final String[] args) throws ConfigurationException {
      final CommandLineParser parser = (CommandLineParser)new PosixParser();
      try {
         SSTableExport.cmd = parser.parse(SSTableExport.options, args);
      }
      catch (ParseException e1) {
         System.err.println(e1.getMessage());
         printUsage();
         System.exit(1);
      }
      if (SSTableExport.cmd.getArgs().length != 1) {
         System.err.println("You must supply exactly one sstable");
         printUsage();
         System.exit(1);
      }
      final String[] keys = SSTableExport.cmd.getOptionValues(KEY_OPTION);
      final HashSet<String> excludes = new HashSet<String>(Arrays.asList((SSTableExport.cmd.getOptionValues("x") == null) ? new String[0] : SSTableExport.cmd.getOptionValues("x")));
      final String ssTableFileName = new File(SSTableExport.cmd.getArgs()[0]).getAbsolutePath();
      if (!new File(ssTableFileName).exists()) {
         System.err.println("Cannot find file " + ssTableFileName);
         System.exit(1);
      }
      final Descriptor desc = Descriptor.fromFilename(ssTableFileName);
      try {
         final TableMetadata metadata = Util.metadataFromSSTable(desc);
         if (SSTableExport.cmd.hasOption(ENUMERATE_KEYS_OPTION)) {
            try (final PartitionIndexIterator iter = desc.getFormat().getReaderFactory().keyIterator(desc, metadata)) {
               JsonTransformer.keysToJson(null, Util.iterToStream(iter), SSTableExport.cmd.hasOption("t"), metadata, System.out);
            }
         }
         else {
            final SSTableReader sstable = SSTableReader.openNoValidation(desc, TableMetadataRef.forOfflineTools(metadata));
            final IPartitioner partitioner = sstable.getPartitioner();
            ISSTableScanner currentScanner;
            if (keys != null && keys.length > 0) {
               List<AbstractBounds<PartitionPosition>> bounds = Arrays.stream(keys).filter(key -> !excludes.contains(key)).
                       map(metadata.partitionKeyType::fromString).
                       map(partitioner::decorateKey).sorted().
                       map(PartitionPosition::getToken).
                       map(token -> new Bounds<>(token.minKeyBound(), token.maxKeyBound())).
                       collect(Collectors.toList());
               currentScanner = sstable.getScanner(bounds.iterator());
            }
            else {
               currentScanner = sstable.getScanner();
            }
            final HashSet set;
            final Stream<UnfilteredRowIterator> partitions = Util.iterToStream(currentScanner).filter(i -> excludes.isEmpty() || !excludes.contains(metadata.partitionKeyType.getString(i.partitionKey().getKey())));
            if (SSTableExport.cmd.hasOption(DEBUG_OUTPUT_OPTION)) {
               AtomicLong atomicLong=new AtomicLong();

               partitions.forEach(partition -> {
                  atomicLong.set(currentScanner.getCurrentPosition());
                  if (!partition.partitionLevelDeletion().isLive()) {
                     System.out.println("[" + metadata.partitionKeyType.getString(partition.partitionKey().getKey()) + "]@" + atomicLong.get() + " " + partition.partitionLevelDeletion());
                  }
                  if (!partition.staticRow().isEmpty()) {
                     System.out.println("[" + metadata.partitionKeyType.getString(partition.partitionKey().getKey()) + "]@" + atomicLong.get() + " " + partition.staticRow().toString(metadata, true));
                  }
                  partition.forEachRemaining(row -> {
                     System.out.println("[" + metadata.partitionKeyType.getString(partition.partitionKey().getKey()) + "]@" + atomicLong.get() + " " + row.toString(metadata, false, true));
                     atomicLong.set(currentScanner.getCurrentPosition());
                  });
                  return;
               });
            }
            else if (SSTableExport.cmd.hasOption(PARTITION_JSON_LINES)) {
               JsonTransformer.toJsonLines(currentScanner, partitions, SSTableExport.cmd.hasOption(RAW_TIMESTAMPS), metadata, System.out);
            }
            else {
               JsonTransformer.toJson(currentScanner, partitions, SSTableExport.cmd.hasOption(RAW_TIMESTAMPS), metadata, System.out);
            }
         }
      }
      catch (IOException e2) {
         e2.printStackTrace(System.err);
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
       Option optKey = new Option(KEY_OPTION, true, "Partition key");
       optKey.setArgs(-2);
       options.addOption(optKey);
       Option excludeKey = new Option(EXCLUDE_KEY_OPTION, true, "Excluded partition key");
       excludeKey.setArgs(-2);
       options.addOption(excludeKey);
       Option optEnumerate = new Option(ENUMERATE_KEYS_OPTION, false, "enumerate partition keys only");
       options.addOption(optEnumerate);
       Option debugOutput = new Option(DEBUG_OUTPUT_OPTION, false, "CQL row per line internal representation");
       options.addOption(debugOutput);
       Option rawTimestamps = new Option(RAW_TIMESTAMPS, false, "Print raw timestamps instead of iso8601 date strings");
       options.addOption(rawTimestamps);
       Option partitionJsonLines = new Option(PARTITION_JSON_LINES, false, "Output json lines, by partition");
       options.addOption(partitionJsonLines);
   }
}
