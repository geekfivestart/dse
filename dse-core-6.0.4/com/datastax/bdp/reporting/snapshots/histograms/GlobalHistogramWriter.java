package com.datastax.bdp.reporting.snapshots.histograms;

import com.datastax.bdp.reporting.CqlWriter;
import com.datastax.bdp.util.QueryProcessorUtil;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import javax.validation.constraints.NotNull;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.BatchStatement.Type;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalHistogramWriter extends CqlWriter<HistogramInfo> {
   private static final String GLOBAL_HISTOGRAM_INSERT_TEMPLATE = "INSERT INTO %s.%s (node_ip, histogram_id, bucket_offset, bucket_count )VALUES (?,?,?,?) USING TTL ?";
   public static final String SUFFIX = "_global";
   private static Logger logger = LoggerFactory.getLogger(GlobalHistogramWriter.class);
   private final String tableName;

   public GlobalHistogramWriter(InetAddress nodeAddress, int ttlSeconds, String tableName) {
      super(nodeAddress, ttlSeconds);
      this.tableName = tableName;
   }

   public void write(@NotNull HistogramInfo writeable) {
      CqlWriter<HistogramInfo>.WriterConfig config = this.getWriterConfig();
      if(config != null) {
         ByteBuffer timestamp = TimestampType.instance.decompose(new Date(System.currentTimeMillis()));
         List<List<ByteBuffer>> perHistogramVars = writeable.toNestedByteBufferList();
         if(perHistogramVars.isEmpty()) {
            return;
         }

         List<ModificationStatement> stmts = new ArrayList();
         ModificationStatement stmt = (ModificationStatement)config.getInsertStatement();
         Iterator var7 = perHistogramVars.iterator();

         while(var7.hasNext()) {
            List<ByteBuffer> histoVars = (List)var7.next();
            histoVars.add(0, timestamp);
            histoVars.add(0, this.nodeAddressBytes);
            histoVars.add(this.getTtlBytes());
            stmts.add(stmt);
         }

         try {
            QueryProcessorUtil.processBatchBlocking(new BatchStatement(-1, Type.UNLOGGED, stmts, Attributes.none()), ConsistencyLevel.ONE, perHistogramVars);
         } catch (Exception var9) {
            handleWriteException(this.getTableName(), var9);
         }
      } else {
         logger.trace("Skipping write to {} because is it not yet setup", this.getTableName());
      }

   }

   protected String getTableName() {
      return this.tableName;
   }

   public String getInsertCQL() {
      return String.format("INSERT INTO %s.%s (node_ip, histogram_id, bucket_offset, bucket_count )VALUES (?,?,?,?) USING TTL ?", new Object[]{"dse_perf", this.tableName});
   }

   protected List<ByteBuffer> getVariables(HistogramInfo writeable) {
      throw new UnsupportedOperationException();
   }
}
