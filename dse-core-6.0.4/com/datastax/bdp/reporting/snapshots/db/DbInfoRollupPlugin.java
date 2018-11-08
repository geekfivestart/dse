package com.datastax.bdp.reporting.snapshots.db;

import com.datastax.bdp.cassandra.cql3.StatementUtils;
import com.datastax.bdp.cassandra.metrics.PerformanceObjectsPlugin;
import com.datastax.bdp.leasemanager.SmallExclusiveTasksPlugin;
import com.datastax.bdp.plugin.DsePlugin;
import com.datastax.bdp.plugin.PerformanceObjectsController;
import com.datastax.bdp.plugin.ThreadPoolPlugin;
import com.datastax.bdp.plugin.bean.SnapshotInfoBean;
import com.datastax.bdp.reporting.snapshots.AbstractScheduledPlugin;
import com.datastax.bdp.util.QueryProcessorUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableIterator;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@DsePlugin(
   dependsOn = {PerformanceObjectsPlugin.class, SmallExclusiveTasksPlugin.class, TableSnapshotPlugin.class, ThreadPoolPlugin.class}
)
public class DbInfoRollupPlugin extends AbstractScheduledPlugin<SnapshotInfoBean> {
   private static final String SELECT_TABLE_SNAPSHOT = String.format("SELECT * FROM %s.%s WHERE node_ip = ?", new Object[]{"dse_perf", "node_table_snapshot"});
   private static final Logger logger = LoggerFactory.getLogger(DbInfoRollupPlugin.class);
   private RollupTableInfoWriter tableRollupWriter;
   private KeyspaceInfoWriter keyspaceRollupWriter;
   @Inject
   private SmallExclusiveTasksPlugin exclusiveTasksPlugin;

   @Inject
   public DbInfoRollupPlugin(PerformanceObjectsController.DbSummaryStatsBean bean, ThreadPoolPlugin threadPool) {
      super(threadPool, bean, true);
   }

   public void setupSchema() {
      this.tableRollupWriter = new RollupTableInfoWriter(this.nodeAddress, this.getTTL() * 3);
      this.tableRollupWriter.createTable();
      this.keyspaceRollupWriter = new KeyspaceInfoWriter(this.nodeAddress, this.getTTL() * 3);
      this.keyspaceRollupWriter.createTable();
   }

   protected int getInitialDelay() {
      return 0;
   }

   protected Runnable getTask() {
      return () -> {
         this.exclusiveTasksPlugin.executeIfLeader(this::doRollup, false, false);
      };
   }

   private void writeSummary(DatabaseInfo dbInfo) {
      logger.debug("Writing aggregate db summary info");
      Iterator var2 = dbInfo.iterator();

      while(var2.hasNext()) {
         KeyspaceInfo ksInfo = (KeyspaceInfo)var2.next();
         logger.debug("Writing aggregate summary for {}", ksInfo.name);
         this.keyspaceRollupWriter.write(ksInfo);
         Iterator var4 = ksInfo.iterator();

         while(var4.hasNext()) {
            TableInfo tableInfo = (TableInfo)var4.next();
            logger.debug("Writing aggregate summary for {}.{}", ksInfo.name, tableInfo.name);
            this.tableRollupWriter.write(tableInfo);
         }
      }

   }

   private void doRollup() {
      try {
         List<Future<DatabaseInfo>> futures = (List)StorageService.instance.getTokenMetadata().getAllEndpoints().stream().map((peerAddress) -> {
            return this.getThreadPool().submit(() -> {
               return this.getTableSummaries(peerAddress);
            });
         }).collect(Collectors.toList());
         List<DatabaseInfo> nodeViews = new ArrayList();
         long timeout = Long.getLong("dse.db_info_rollup_node_query_timeout", 3000L).longValue();
         Iterator var5 = futures.iterator();

         while(var5.hasNext()) {
            Future future = (Future)var5.next();

            try {
               nodeViews.add(future.get(timeout, TimeUnit.MILLISECONDS));
            } catch (InterruptedException var8) {
               Thread.currentThread().interrupt();
               throw var8;
            } catch (Exception var9) {
               logger.info("Error retrieving node level db summary: {}", var9.getMessage());
            }
         }

         DatabaseInfo aggregateView = new DatabaseInfo();
         aggregateView.aggregate(nodeViews);
         this.writeSummary(aggregateView);
      } catch (Exception var10) {
         logger.warn("Caught exception while rolling up db info", var10);
      }

   }

   private DatabaseInfo getTableSummaries(InetAddress peerAddress) {
      logger.debug("Fetching CQL table summaries from {}", peerAddress);
      DatabaseInfo dbInfo = new DatabaseInfo(peerAddress.getHostAddress());
      CQLStatement stmt = StatementUtils.prepareStatementBlocking(SELECT_TABLE_SNAPSHOT, QueryState.forInternalCalls());
      UntypedResultSet rows = QueryProcessorUtil.processPreparedSelect(stmt, ConsistencyLevel.ONE, Collections.singletonList(ByteBufferUtil.bytes(peerAddress)));
      UnmodifiableIterator var5 = ImmutableList.copyOf(rows.iterator()).iterator();

      while(var5.hasNext()) {
         Row row = (Row)var5.next();
         TableInfo tableInfo = TableInfo.fromRow(row);
         logger.debug("Reading summary of {}.{} from {}", new Object[]{tableInfo.ksName, tableInfo.name, peerAddress});
         dbInfo.addTableInfo(TableInfo.fromRow(row));
      }

      return dbInfo;
   }
}
