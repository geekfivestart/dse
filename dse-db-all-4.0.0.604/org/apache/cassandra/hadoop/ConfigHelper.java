package org.apache.cassandra.hadoop;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigHelper {
   private static final String INPUT_PARTITIONER_CONFIG = "cassandra.input.partitioner.class";
   private static final String OUTPUT_PARTITIONER_CONFIG = "cassandra.output.partitioner.class";
   private static final String INPUT_KEYSPACE_CONFIG = "cassandra.input.keyspace";
   private static final String OUTPUT_KEYSPACE_CONFIG = "cassandra.output.keyspace";
   private static final String INPUT_KEYSPACE_USERNAME_CONFIG = "cassandra.input.keyspace.username";
   private static final String INPUT_KEYSPACE_PASSWD_CONFIG = "cassandra.input.keyspace.passwd";
   private static final String OUTPUT_KEYSPACE_USERNAME_CONFIG = "cassandra.output.keyspace.username";
   private static final String OUTPUT_KEYSPACE_PASSWD_CONFIG = "cassandra.output.keyspace.passwd";
   private static final String INPUT_COLUMNFAMILY_CONFIG = "cassandra.input.columnfamily";
   private static final String OUTPUT_COLUMNFAMILY_CONFIG = "mapreduce.output.basename";
   private static final String INPUT_PREDICATE_CONFIG = "cassandra.input.predicate";
   private static final String INPUT_KEYRANGE_CONFIG = "cassandra.input.keyRange";
   private static final String INPUT_SPLIT_SIZE_CONFIG = "cassandra.input.split.size";
   private static final String INPUT_SPLIT_SIZE_IN_MB_CONFIG = "cassandra.input.split.size_mb";
   private static final String INPUT_WIDEROWS_CONFIG = "cassandra.input.widerows";
   private static final int DEFAULT_SPLIT_SIZE = 65536;
   private static final String RANGE_BATCH_SIZE_CONFIG = "cassandra.range.batch.size";
   private static final int DEFAULT_RANGE_BATCH_SIZE = 4096;
   private static final String INPUT_INITIAL_ADDRESS = "cassandra.input.address";
   private static final String OUTPUT_INITIAL_ADDRESS = "cassandra.output.address";
   private static final String READ_CONSISTENCY_LEVEL = "cassandra.consistencylevel.read";
   private static final String WRITE_CONSISTENCY_LEVEL = "cassandra.consistencylevel.write";
   private static final String OUTPUT_COMPRESSION_CLASS = "cassandra.output.compression.class";
   private static final String OUTPUT_COMPRESSION_CHUNK_LENGTH = "cassandra.output.compression.length";
   private static final String OUTPUT_LOCAL_DC_ONLY = "cassandra.output.local.dc.only";
   private static final Logger logger = LoggerFactory.getLogger(ConfigHelper.class);

   public ConfigHelper() {
   }

   public static void setInputColumnFamily(Configuration conf, String keyspace, String columnFamily, boolean widerows) {
      if(keyspace == null) {
         throw new UnsupportedOperationException("keyspace may not be null");
      } else if(columnFamily == null) {
         throw new UnsupportedOperationException("table may not be null");
      } else {
         conf.set("cassandra.input.keyspace", keyspace);
         conf.set("cassandra.input.columnfamily", columnFamily);
         conf.set("cassandra.input.widerows", String.valueOf(widerows));
      }
   }

   public static void setInputColumnFamily(Configuration conf, String keyspace, String columnFamily) {
      setInputColumnFamily(conf, keyspace, columnFamily, false);
   }

   public static void setOutputKeyspace(Configuration conf, String keyspace) {
      if(keyspace == null) {
         throw new UnsupportedOperationException("keyspace may not be null");
      } else {
         conf.set("cassandra.output.keyspace", keyspace);
      }
   }

   public static void setOutputColumnFamily(Configuration conf, String columnFamily) {
      conf.set("mapreduce.output.basename", columnFamily);
   }

   public static void setOutputColumnFamily(Configuration conf, String keyspace, String columnFamily) {
      setOutputKeyspace(conf, keyspace);
      setOutputColumnFamily(conf, columnFamily);
   }

   public static void setRangeBatchSize(Configuration conf, int batchsize) {
      conf.setInt("cassandra.range.batch.size", batchsize);
   }

   public static int getRangeBatchSize(Configuration conf) {
      return conf.getInt("cassandra.range.batch.size", 4096);
   }

   public static void setInputSplitSize(Configuration conf, int splitsize) {
      conf.setInt("cassandra.input.split.size", splitsize);
   }

   public static int getInputSplitSize(Configuration conf) {
      return conf.getInt("cassandra.input.split.size", 65536);
   }

   public static void setInputSplitSizeInMb(Configuration conf, int splitSizeMb) {
      conf.setInt("cassandra.input.split.size_mb", splitSizeMb);
   }

   public static int getInputSplitSizeInMb(Configuration conf) {
      return conf.getInt("cassandra.input.split.size_mb", -1);
   }

   public static void setInputRange(Configuration conf, String startToken, String endToken) {
      conf.set("cassandra.input.keyRange", startToken + "," + endToken);
   }

   public static Pair<String, String> getInputKeyRange(Configuration conf) {
      String str = conf.get("cassandra.input.keyRange");
      if(str == null) {
         return null;
      } else {
         String[] parts = str.split(",");

         assert parts.length == 2;

         return Pair.create(parts[0], parts[1]);
      }
   }

   public static String getInputKeyspace(Configuration conf) {
      return conf.get("cassandra.input.keyspace");
   }

   public static String getOutputKeyspace(Configuration conf) {
      return conf.get("cassandra.output.keyspace");
   }

   public static void setInputKeyspaceUserNameAndPassword(Configuration conf, String username, String password) {
      setInputKeyspaceUserName(conf, username);
      setInputKeyspacePassword(conf, password);
   }

   public static void setInputKeyspaceUserName(Configuration conf, String username) {
      conf.set("cassandra.input.keyspace.username", username);
   }

   public static String getInputKeyspaceUserName(Configuration conf) {
      return conf.get("cassandra.input.keyspace.username");
   }

   public static void setInputKeyspacePassword(Configuration conf, String password) {
      conf.set("cassandra.input.keyspace.passwd", password);
   }

   public static String getInputKeyspacePassword(Configuration conf) {
      return conf.get("cassandra.input.keyspace.passwd");
   }

   public static void setOutputKeyspaceUserNameAndPassword(Configuration conf, String username, String password) {
      setOutputKeyspaceUserName(conf, username);
      setOutputKeyspacePassword(conf, password);
   }

   public static void setOutputKeyspaceUserName(Configuration conf, String username) {
      conf.set("cassandra.output.keyspace.username", username);
   }

   public static String getOutputKeyspaceUserName(Configuration conf) {
      return conf.get("cassandra.output.keyspace.username");
   }

   public static void setOutputKeyspacePassword(Configuration conf, String password) {
      conf.set("cassandra.output.keyspace.passwd", password);
   }

   public static String getOutputKeyspacePassword(Configuration conf) {
      return conf.get("cassandra.output.keyspace.passwd");
   }

   public static String getInputColumnFamily(Configuration conf) {
      return conf.get("cassandra.input.columnfamily");
   }

   public static String getOutputColumnFamily(Configuration conf) {
      if(conf.get("mapreduce.output.basename") != null) {
         return conf.get("mapreduce.output.basename");
      } else {
         throw new UnsupportedOperationException("You must set the output column family using either setOutputColumnFamily or by adding a named output with MultipleOutputs");
      }
   }

   public static boolean getInputIsWide(Configuration conf) {
      return Boolean.parseBoolean(conf.get("cassandra.input.widerows"));
   }

   public static String getReadConsistencyLevel(Configuration conf) {
      return conf.get("cassandra.consistencylevel.read", "LOCAL_ONE");
   }

   public static void setReadConsistencyLevel(Configuration conf, String consistencyLevel) {
      conf.set("cassandra.consistencylevel.read", consistencyLevel);
   }

   public static String getWriteConsistencyLevel(Configuration conf) {
      return conf.get("cassandra.consistencylevel.write", "LOCAL_ONE");
   }

   public static void setWriteConsistencyLevel(Configuration conf, String consistencyLevel) {
      conf.set("cassandra.consistencylevel.write", consistencyLevel);
   }

   public static String getInputInitialAddress(Configuration conf) {
      return conf.get("cassandra.input.address");
   }

   public static void setInputInitialAddress(Configuration conf, String address) {
      conf.set("cassandra.input.address", address);
   }

   public static void setInputPartitioner(Configuration conf, String classname) {
      conf.set("cassandra.input.partitioner.class", classname);
   }

   public static IPartitioner getInputPartitioner(Configuration conf) {
      return FBUtilities.newPartitioner(conf.get("cassandra.input.partitioner.class"));
   }

   public static String getOutputInitialAddress(Configuration conf) {
      return conf.get("cassandra.output.address");
   }

   public static void setOutputInitialAddress(Configuration conf, String address) {
      conf.set("cassandra.output.address", address);
   }

   public static void setOutputPartitioner(Configuration conf, String classname) {
      conf.set("cassandra.output.partitioner.class", classname);
   }

   public static IPartitioner getOutputPartitioner(Configuration conf) {
      return FBUtilities.newPartitioner(conf.get("cassandra.output.partitioner.class"));
   }

   public static String getOutputCompressionClass(Configuration conf) {
      return conf.get("cassandra.output.compression.class");
   }

   public static String getOutputCompressionChunkLength(Configuration conf) {
      return conf.get("cassandra.output.compression.length", String.valueOf(65536));
   }

   public static void setOutputCompressionClass(Configuration conf, String classname) {
      conf.set("cassandra.output.compression.class", classname);
   }

   public static void setOutputCompressionChunkLength(Configuration conf, String length) {
      conf.set("cassandra.output.compression.length", length);
   }

   public static boolean getOutputLocalDCOnly(Configuration conf) {
      return Boolean.parseBoolean(conf.get("cassandra.output.local.dc.only", "false"));
   }

   public static void setOutputLocalDCOnly(Configuration conf, boolean localDCOnly) {
      conf.set("cassandra.output.local.dc.only", Boolean.toString(localDCOnly));
   }
}
