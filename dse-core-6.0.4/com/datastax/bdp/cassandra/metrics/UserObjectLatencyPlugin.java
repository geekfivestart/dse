package com.datastax.bdp.cassandra.metrics;

import com.datastax.bdp.cassandra.cql3.DseQueryOperationFactory;
import com.datastax.bdp.cassandra.cql3.StatementUtils;
import com.datastax.bdp.ioc.UserLatencyMetricsWriterProvider;
import com.datastax.bdp.plugin.DsePlugin;
import com.datastax.bdp.plugin.ThreadPoolPlugin;
import com.datastax.bdp.plugin.bean.UserLatencyTrackingBean;
import com.datastax.bdp.reporting.snapshots.AbstractScheduledPlugin;
import com.datastax.bdp.system.PerformanceObjectsKeyspace;
import com.datastax.bdp.util.SchemaTool;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLStatementUtils;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.service.ClientState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@DsePlugin(
   dependsOn = {PerformanceObjectsPlugin.class, ThreadPoolPlugin.class}
)
public class UserObjectLatencyPlugin extends AbstractScheduledPlugin<UserLatencyTrackingBean> {
   private static final Logger logger = LoggerFactory.getLogger(UserObjectLatencyPlugin.class);
   private volatile UserLatencyMetricsWriter writer;
   private final UserLatencyMetricsWriterProvider userLatencyMetricsWriterProvider;
   private final PropertyChangeListener limitListener = new PropertyChangeListener() {
      public void propertyChange(PropertyChangeEvent evt) {
         UserObjectLatencyPlugin.this.writer.setTopStatsLimit(((Integer)evt.getNewValue()).intValue());
      }
   };

   @Inject
   public UserObjectLatencyPlugin(UserLatencyMetricsWriterProvider userLatencyMetricsWriterProvider, UserLatencyTrackingBean mbean, ThreadPoolPlugin threadPool) {
      super(threadPool, mbean, true);
      this.userLatencyMetricsWriterProvider = userLatencyMetricsWriterProvider;
      logger.debug("Initialized user/object io tracker plugin");
   }

   public synchronized void onActivate() {
      this.writer = this.userLatencyMetricsWriterProvider.get();
      super.onActivate();
      ((UserLatencyTrackingBean)this.getMbean()).addPropertyChangeListener("topStatsLimit", this.limitListener);
   }

   public synchronized void onPreDeactivate() {
      ((UserLatencyTrackingBean)this.getMbean()).removePropertyChangeListener("topStatsLimit", this.limitListener);
      super.onPreDeactivate();
   }

   public void setupSchema() {
      PerformanceObjectsKeyspace.maybeCreateTable("user_object_read_io_snapshot");
      PerformanceObjectsKeyspace.maybeCreateTable("object_user_read_io_snapshot");
      PerformanceObjectsKeyspace.maybeCreateTable("user_read_io_snapshot");
      PerformanceObjectsKeyspace.maybeCreateTable("user_object_write_io_snapshot");
      PerformanceObjectsKeyspace.maybeCreateTable("object_user_write_io_snapshot");
      PerformanceObjectsKeyspace.maybeCreateTable("user_write_io_snapshot");
      PerformanceObjectsKeyspace.maybeCreateTable("user_object_io");
      PerformanceObjectsKeyspace.maybeCreateTable("object_user_io");
      PerformanceObjectsKeyspace.maybeCreateTable("user_io");
      String[] tables = new String[]{"user_object_write_io_snapshot", "user_object_read_io_snapshot", "user_object_io", "object_user_write_io_snapshot", "object_user_read_io_snapshot", "object_user_io"};
      String[] var2 = tables;
      int var3 = tables.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         String table = var2[var4];
         SchemaTool.maybeAddNewColumn("dse_perf", table, "read_quantiles", String.format("ALTER TABLE dse_perf.%s ADD read_quantiles map<double, double>;", new Object[]{table}));
         SchemaTool.maybeAddNewColumn("dse_perf", table, "write_quantiles", String.format("ALTER TABLE dse_perf.%s ADD write_quantiles map<double, double>;", new Object[]{table}));
      }

   }

   protected Runnable getTask() {
      return this.writer;
   }

   public void maybeRecordOperationMetrics(ClientState state, LatencyValues.EventType interactionType, String columnFamily, long durationNanos) {
      if(this.isEnabled() && state.getRemoteAddress() != null) {
         this.recordEvent(interactionType, columnFamily, durationNanos, state.getRemoteAddress(), state.getUser().getName(), state.getRawKeyspace(), TimeUnit.NANOSECONDS);
      }

   }

   private void maybeRecordOperationMetrics(ClientState state, BatchStatement batch, long duration) {
      if(this.isEnabled() && state.getRemoteAddress() != null) {
         Iterator var5 = batch.getStatements().iterator();

         while(var5.hasNext()) {
            ModificationStatement stmt = (ModificationStatement)var5.next();
            this.recordEvent(LatencyValues.EventType.WRITE, CQLStatementUtils.getTable(stmt), duration, state.getRemoteAddress(), state.getUser().getName(), CQLStatementUtils.getKeyspace(stmt), TimeUnit.MILLISECONDS);
         }
      }

   }

   public void maybeRecordOperationMetrics(DseQueryOperationFactory.Operation<? extends CQLStatement, ?> operation, long duration) {
      if(this.isEnabled()) {
         if(operation.getStatement() instanceof BatchStatement) {
            this.maybeRecordOperationMetrics(operation.getQueryState().getClientState(), (BatchStatement)operation.getStatement(), duration);
         } else {
            ClientState state = operation.getQueryState().getClientState();
            InetSocketAddress clientAddress = state.getRemoteAddress();
            if(clientAddress != null) {
               LatencyValues.EventType interactionType = StatementUtils.getInteractionType(operation.getStatement());
               if(interactionType != null) {
                  this.recordEvent(interactionType, CQLStatementUtils.getTable(operation.getStatement()), duration, clientAddress, state.getUser().getName(), CQLStatementUtils.getKeyspace(operation.getStatement()), TimeUnit.MILLISECONDS);
               }
            }
         }
      }
   }

   private void recordEvent(LatencyValues.EventType interactionType, String columnFamily, long durationNanos, InetSocketAddress remoteAddress, String name, String rawKeyspace, TimeUnit timeUnit) {
      this.writer.getUserMetrics().recordLatencyEvent(remoteAddress, name, rawKeyspace, columnFamily, interactionType, durationNanos, timeUnit);
   }
}
