package org.apache.cassandra.hadoop.cql3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

public class CqlBulkOutputFormat extends OutputFormat<Object, List<ByteBuffer>> implements org.apache.hadoop.mapred.OutputFormat<Object, List<ByteBuffer>> {
   private static final String OUTPUT_CQL_SCHEMA_PREFIX = "cassandra.table.schema.";
   private static final String OUTPUT_CQL_INSERT_PREFIX = "cassandra.table.insert.";
   private static final String DELETE_SOURCE = "cassandra.output.delete.source";
   private static final String TABLE_ALIAS_PREFIX = "cqlbulkoutputformat.table.alias.";

   public CqlBulkOutputFormat() {
   }

   /** @deprecated */
   @Deprecated
   public CqlBulkRecordWriter getRecordWriter(FileSystem filesystem, JobConf job, String name, Progressable progress) throws IOException {
      return new CqlBulkRecordWriter(job, progress);
   }

   public CqlBulkRecordWriter getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
      return new CqlBulkRecordWriter(context);
   }

   public void checkOutputSpecs(JobContext context) {
      this.checkOutputSpecs(HadoopCompat.getConfiguration(context));
   }

   private void checkOutputSpecs(Configuration conf) {
      if(ConfigHelper.getOutputKeyspace(conf) == null) {
         throw new UnsupportedOperationException("you must set the keyspace with setTable()");
      }
   }

   /** @deprecated */
   @Deprecated
   public void checkOutputSpecs(FileSystem filesystem, JobConf job) throws IOException {
      this.checkOutputSpecs((Configuration)job);
   }

   public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
      return new CqlBulkOutputFormat.NullOutputCommitter();
   }

   public static void setTableSchema(Configuration conf, String columnFamily, String schema) {
      conf.set("cassandra.table.schema." + columnFamily, schema);
   }

   public static void setTableInsertStatement(Configuration conf, String columnFamily, String insertStatement) {
      conf.set("cassandra.table.insert." + columnFamily, insertStatement);
   }

   public static String getTableSchema(Configuration conf, String columnFamily) {
      String schema = conf.get("cassandra.table.schema." + columnFamily);
      if(schema == null) {
         throw new UnsupportedOperationException("You must set the Table schema using setTableSchema.");
      } else {
         return schema;
      }
   }

   public static String getTableInsertStatement(Configuration conf, String columnFamily) {
      String insert = conf.get("cassandra.table.insert." + columnFamily);
      if(insert == null) {
         throw new UnsupportedOperationException("You must set the Table insert statement using setTableSchema.");
      } else {
         return insert;
      }
   }

   public static void setDeleteSourceOnSuccess(Configuration conf, boolean deleteSrc) {
      conf.setBoolean("cassandra.output.delete.source", deleteSrc);
   }

   public static boolean getDeleteSourceOnSuccess(Configuration conf) {
      return conf.getBoolean("cassandra.output.delete.source", false);
   }

   public static void setTableAlias(Configuration conf, String alias, String columnFamily) {
      conf.set("cqlbulkoutputformat.table.alias." + alias, columnFamily);
   }

   public static String getTableForAlias(Configuration conf, String alias) {
      return conf.get("cqlbulkoutputformat.table.alias." + alias);
   }

   public static void setIgnoreHosts(Configuration conf, String ignoreNodesCsv) {
      conf.set("mapreduce.output.bulkoutputformat.ignorehosts", ignoreNodesCsv);
   }

   public static void setIgnoreHosts(Configuration conf, String... ignoreNodes) {
      conf.setStrings("mapreduce.output.bulkoutputformat.ignorehosts", ignoreNodes);
   }

   public static Collection<String> getIgnoreHosts(Configuration conf) {
      return conf.getStringCollection("mapreduce.output.bulkoutputformat.ignorehosts");
   }

   public static class NullOutputCommitter extends OutputCommitter {
      public NullOutputCommitter() {
      }

      public void abortTask(TaskAttemptContext taskContext) {
      }

      public void cleanupJob(JobContext jobContext) {
      }

      public void commitTask(TaskAttemptContext taskContext) {
      }

      public boolean needsTaskCommit(TaskAttemptContext taskContext) {
         return false;
      }

      public void setupJob(JobContext jobContext) {
      }

      public void setupTask(TaskAttemptContext taskContext) {
      }
   }
}
