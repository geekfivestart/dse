package com.datastax.bdp.cassandra.cql3;

import com.datastax.bdp.cassandra.metrics.PercentileFilter;
import com.datastax.bdp.cassandra.metrics.PerformanceObjectsPlugin;
import com.datastax.bdp.plugin.AbstractPlugin;
import com.datastax.bdp.plugin.CqlSlowLogMXBean;
import com.datastax.bdp.plugin.DsePlugin;
import com.datastax.bdp.plugin.PerformanceObjectsController;
import com.datastax.bdp.plugin.ThreadPoolPlugin;
import com.datastax.bdp.server.DseDaemon;
import com.datastax.bdp.system.PerformanceObjectsKeyspace;
import com.datastax.bdp.util.SchemaTool;
import com.datastax.bdp.util.rpc.RpcRegistry;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.beans.PropertyChangeListener;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLStatementUtils;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement.Prepared;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@DsePlugin(
   dependsOn = {PerformanceObjectsPlugin.class, ThreadPoolPlugin.class}
)
public class CqlSlowLogPlugin extends AbstractPlugin {
   private static final Logger logger = LoggerFactory.getLogger(CqlSlowLogPlugin.class);
   private final PerformanceObjectsController.CqlSlowLogBean bean;
   private final ThreadPoolPlugin threadPool;
   private final CqlSlowLogWriter writer;
   private final PercentileFilter percentile;
   private final AtomicReference<ScheduledFuture<?>> thresholdRefresher = new AtomicReference();

   @Inject
   public CqlSlowLogPlugin(PerformanceObjectsController.CqlSlowLogBean bean, ThreadPoolPlugin threadPool, PercentileFilter percentile) {
      this.bean = bean;
      this.threadPool = threadPool;
      logger.debug("Initializing CQL slow query log plugin");
      this.percentile = percentile;
      percentile.setPercentile(bean.getThreshold());
      percentile.setMinimumSamples(bean.getMinimumSamples());
      bean.addPropertyChangeListener("minimumSamples", (evt) -> {
         percentile.setMinimumSamples(((Long)evt.getNewValue()).longValue());
         this.adjustRefresh();
         this.adjustEffectiveThreshold();
      });
      this.adjustEffectiveThreshold();
      bean.addPropertyChangeListener("threshold", (evt) -> {
         this.adjustEffectiveThreshold();
         this.adjustRefresh();
      });
      this.writer = new CqlSlowLogWriter(threadPool);
   }

   private static List<String> decomposeBatchStatement(BatchQueryOptions queryOptions) {
      List<String> cqlStrings = new ArrayList();
      List<Object> queryOrIdList = queryOptions.getQueryOrIdList();
      Iterator var3 = queryOrIdList.iterator();

      while(var3.hasNext()) {
         Object queryOrId = var3.next();
         if(queryOrId instanceof String) {
            cqlStrings.add((String)queryOrId);
         } else if(queryOrId instanceof MD5Digest) {
            Prepared prepared = ClientState.getCQLQueryHandler().getPrepared((MD5Digest)queryOrId);
            cqlStrings.add(prepared.rawCQLStatement);
         }
      }

      return cqlStrings;
   }

   public void onRegister() {
      super.onRegister();
      this.bean.activate(this);
   }

   public void setupSchema() {
      PerformanceObjectsKeyspace.maybeCreateTable("node_slow_log");
      SchemaTool.maybeAddNewColumn("dse_perf", "node_slow_log", "tracing_session_id", PerformanceObjectsKeyspace.SCHEMA_ADD_NODE_SLOW_LOG_TRACING_SESSION_ID);
   }

   public synchronized void onActivate() {
      this.writer.activate();
      this.adjustRefresh();
      RpcRegistry.register(PerformanceObjectsController.getPerfBeanName(this.bean.getClass()), this.bean);
   }

   private void adjustRefresh() {
      if(this.getThreshold() <= 1.0D && this.percentile.isWarmedUp()) {
         this.enableRefresh();
      } else {
         this.disableRefresh();
      }

   }

   private void enableRefresh() {
      if(this.thresholdRefresher.get() == null) {
         ScheduledFuture<?> newJob = this.threadPool.scheduleAtFixedRate(this::adjustEffectiveThreshold, 0L, 1L, TimeUnit.SECONDS);
         if(this.thresholdRefresher.compareAndSet((Object)null, newJob)) {
            logger.info("Background refresh enabled");
         } else {
            newJob.cancel(true);
         }
      }

   }

   private void disableRefresh() {
      ScheduledFuture<?> refresher = (ScheduledFuture)this.thresholdRefresher.getAndUpdate((current) -> {
         return null;
      });
      if(refresher != null) {
         logger.info("Cancelling background refresh");
         refresher.cancel(true);
      }

   }

   private void adjustEffectiveThreshold() {
      double maybePercent = this.getThreshold();
      long absolute;
      if(maybePercent > 1.0D) {
         absolute = (long)maybePercent;
      } else {
         this.percentile.setPercentile(maybePercent);
         absolute = this.percentile.isWarmedUp()?(long)this.percentile.getPercentileValue():9223372036854775807L;
      }

      if(absolute != this.bean.getEffectiveThreshold()) {
         logger.debug("changing to {}", Long.valueOf(absolute));
         this.bean.setEffectiveThreshold(absolute);
      }

   }

   public synchronized void onPreDeactivate() {
      RpcRegistry.unregister(PerformanceObjectsController.getPerfBeanName(this.bean.getClass()));
      this.disableRefresh();
      super.onPreDeactivate();
   }

   public boolean isEnabled() {
      return this.bean.isEnabled();
   }

   public double getThreshold() {
      return this.bean.getThreshold();
   }

   static InetSocketAddress getClientAddress(ClientState clientState) {
      return clientState.getRemoteAddress() != null?clientState.getRemoteAddress():new InetSocketAddress(DseDaemon.UNKNOWN_SOURCE, 0);
   }

   private void record(List<String> cql, Set<Pair<String, String>> keyspaceTablePairs, QueryState state, UUID startTime, long duration, UUID tracingSessionId) {
      try {
         logger.debug("Recording slow query");
         InetAddress sourceIp = getClientAddress(state.getClientState()).getAddress();
         String username = state.getClientState().getUser().getName();
         Set<String> keyspaceAndTableNames = (Set)keyspaceTablePairs.stream().map((pair) -> {
            return pair.left != null?String.format("%s.%s", new Object[]{pair.left, pair.right}):(String)pair.right;
         }).collect(Collectors.toSet());
         String tablesString = keyspaceAndTableNames.isEmpty()?"":keyspaceAndTableNames.toString();
         String cqlStrings = cql.isEmpty()?"":cql.toString();
         ((Queue)this.bean.slowestQueries.get()).add(new CqlSlowLogMXBean.SlowCqlQuery(tablesString, sourceIp.toString(), username, startTime.toString(), duration, cqlStrings, null == tracingSessionId?"":tracingSessionId.toString()));
         if(this.bean.isSkipWritingToDB()) {
            logger.debug("Not writing slow query to DB");
         } else {
            this.writer.recordSlowOperation(keyspaceTablePairs, sourceIp, username, startTime, duration, cql, tracingSessionId);
         }
      } catch (Exception var13) {
         logger.debug("Caught exception when writing to cql slow log", var13);
      }

   }

   public void maybeRecord(DseQueryOperationFactory.Operation<? extends CQLStatement, ?> operation, UUID startTime, long duration, UUID tracingSessionId) {
      if(this.isEnabled() && !operation.isInternal()) {
         if(this.percentile.update(duration)) {
            this.adjustRefresh();
         }

         if(duration >= this.bean.getEffectiveThreshold()) {
            Set<Pair<String, String>> keyspaceTablePairs = new HashSet();
            List cql;
            if(operation instanceof DseStandardQueryOperationFactory.BatchOperation) {
               cql = decomposeBatchStatement((BatchQueryOptions)operation.getOptions());
               Iterator var8 = ((BatchStatement)operation.getStatement()).getStatements().iterator();

               while(var8.hasNext()) {
                  CQLStatement stmt = (CQLStatement)var8.next();
                  ((Set)keyspaceTablePairs).add(Pair.create(CQLStatementUtils.getKeyspace(stmt), CQLStatementUtils.getTable(stmt)));
               }
            } else {
               keyspaceTablePairs = Collections.singleton(Pair.create(CQLStatementUtils.getKeyspace(operation.getStatement()), operation.getTableName()));
               cql = Collections.singletonList(operation.getCql());
            }

            if(this.hasKeyspaceThatShouldBeRecorded((Set)keyspaceTablePairs)) {
               this.record(cql, (Set)keyspaceTablePairs, operation.getQueryState(), startTime, duration, tracingSessionId);
            } else {
               logger.debug("Skipping slow log as statement only affects untracked keyspaces");
            }

         }
      }
   }

   private boolean hasKeyspaceThatShouldBeRecorded(Set<Pair<String, String>> tables) {
      return tables.stream().anyMatch((pair) -> {
         return !PerformanceObjectsPlugin.isUntracked((String)pair.left);
      });
   }
}
