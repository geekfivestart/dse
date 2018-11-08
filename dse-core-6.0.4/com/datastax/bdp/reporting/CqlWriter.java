package com.datastax.bdp.reporting;

import com.datastax.bdp.cassandra.cql3.StatementUtils;
import com.datastax.bdp.gms.DseVersionNotifier;
import com.datastax.bdp.server.SystemInfo;
import com.datastax.bdp.snitch.EndpointStateTracker;
import com.datastax.bdp.system.PerformanceObjectsKeyspace;
import com.datastax.bdp.util.DseUtil;
import com.datastax.bdp.util.QueryProcessorUtil;
import com.datastax.bdp.util.SchemaTool;
import com.google.common.base.Throwables;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.validation.constraints.NotNull;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CqlWriter<T extends CqlWritable> {
   private static final Logger logger = LoggerFactory.getLogger(CqlWriter.class);
   private volatile CqlWriter<T>.WriterConfig writerConfig;
   protected final ByteBuffer nodeAddressBytes;
   private final AtomicReference<ByteBuffer> ttlBytes = new AtomicReference();

   public CqlWriter(InetAddress nodeAddress, int ttlSeconds) {
      this.nodeAddressBytes = ByteBufferUtil.bytes(nodeAddress);
      this.setTtl(ttlSeconds);
   }

   protected abstract String getTableName();

   protected abstract String getInsertCQL();

   protected List<ByteBuffer> getVariables(T writeable) {
      return new ArrayList(0);
   }

   public void setTtl(int ttlSeconds) {
      this.ttlBytes.set(ByteBufferUtil.bytes(ttlSeconds));
   }

   public final void createTable() {
      DseVersionNotifier notifier = EndpointStateTracker.instance.getVersionNotifier();
      CassandraVersion minVersion = notifier.getMinVersion();
      this.createTable(minVersion);
      if(!DseUtil.majorMinorVersionEquals(minVersion, SystemInfo.getDseVersion())) {
         notifier.addObserver(this::createTable, SystemInfo.getDseVersion());
      }

   }

   public void createTable(CassandraVersion dseVersion) {
      if(DseUtil.majorMinorVersionEquals(dseVersion, SystemInfo.getDseVersion())) {
         logger.info("DSE version is " + dseVersion + ", running normal mode");
         PerformanceObjectsKeyspace.maybeCreateTable(this.getTableName());
         this.maybeAlterSchema();
         this.writerConfig = this.createWriterConfig(dseVersion);
      } else {
         TableMetadata existingTable = SchemaTool.getTableMetadata("dse_perf", this.getTableName());
         if(existingTable != null) {
            logger.info("Table {} will be used in legacy mode until all nodes are running DSE {}", this.getTableName(), SystemInfo.getDseVersion());
            this.writerConfig = this.createWriterConfig(dseVersion);
         } else {
            logger.info("Table {} does not exist, writes will be disabled until all nodes are running DSE {}", this.getTableName(), SystemInfo.getDseVersion());
            this.writerConfig = null;
         }
      }

   }

   protected CqlWriter<T>.WriterConfig createWriterConfig(CassandraVersion dseVersion) {
      return new CqlWriter.WriterConfig(this.getInsertCQL(), this::getVariables);
   }

   public void maybeAlterSchema() {
   }

   public void write(@NotNull T writeable) {
      CqlWriter<T>.WriterConfig config = this.getWriterConfig();
      if(config != null) {
         try {
            if(logger.isDebugEnabled()) {
               logger.trace("Writing to " + this.getTableName() + "...");
            }

            QueryProcessorUtil.processPreparedBlocking(config.getInsertStatement(), ConsistencyLevel.ONE, (List)config.variablesProvider.apply(writeable));
         } catch (Exception var4) {
            handleWriteException(this.getTableName(), var4);
         }
      } else {
         logger.trace("Skipping write to {} because is it not yet setup", this.getTableName());
      }

   }

   public static void handleWriteException(String tableName, Throwable e) {
      if(!(e instanceof RejectedExecutionException)) {
         if(!(e instanceof RequestExecutionException) && !(e instanceof RequestValidationException)) {
            Throwables.propagate(e);
         } else {
            logger.error("Error writing to db table " + tableName, e);
         }
      }

   }

   public ByteBuffer getTtlBytes() {
      return (ByteBuffer)this.ttlBytes.get();
   }

   public CqlWriter<T>.WriterConfig getWriterConfig() {
      return this.writerConfig;
   }

   protected class WriterConfig {
      public final String insert;
      public final Function<T, List<ByteBuffer>> variablesProvider;

      public WriterConfig(String this$0, Function<T, List<ByteBuffer>> insert) {
         this.insert = insert;
         this.variablesProvider = variablesProvider;
      }

      public CQLStatement getInsertStatement() {
         return this.insert != null?StatementUtils.prepareStatementBlocking(this.insert, QueryState.forInternalCalls(), "Could not prepare insert statement for performance objects table " + CqlWriter.this.getTableName()):null;
      }
   }
}
