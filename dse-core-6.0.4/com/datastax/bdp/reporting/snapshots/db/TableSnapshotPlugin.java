package com.datastax.bdp.reporting.snapshots.db;

import com.datastax.bdp.cassandra.metrics.PerformanceObjectsPlugin;
import com.datastax.bdp.plugin.DsePlugin;
import com.datastax.bdp.plugin.PerformanceObjectsController;
import com.datastax.bdp.plugin.ThreadPoolPlugin;
import com.datastax.bdp.plugin.bean.SnapshotInfoBean;
import com.datastax.bdp.reporting.snapshots.AbstractScheduledPlugin;
import com.google.common.collect.UnmodifiableIterator;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.Iterator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@DsePlugin(
   dependsOn = {PerformanceObjectsPlugin.class, ThreadPoolPlugin.class}
)
public class TableSnapshotPlugin extends AbstractScheduledPlugin<SnapshotInfoBean> {
   private PerNodeTableInfoWriter writer;
   private static final Logger logger = LoggerFactory.getLogger(TableSnapshotPlugin.class);

   @Inject
   public TableSnapshotPlugin(PerformanceObjectsController.DbSummaryStatsBean bean, ThreadPoolPlugin threadPool) {
      super(threadPool, bean, true);
   }

   public void setupSchema() {
      this.writer = new PerNodeTableInfoWriter(this.nodeAddress, this.getTTL());
      this.writer.createTable();
   }

   protected int getInitialDelay() {
      return 0;
   }

   protected Runnable getTask() {
      return this::update;
   }

   private void update() {
      try {
         UnmodifiableIterator var1 = Schema.instance.getNonSystemKeyspaces().iterator();

         while(true) {
            String ksname;
            do {
               if(!var1.hasNext()) {
                  return;
               }

               ksname = (String)var1.next();
            } while(PerformanceObjectsPlugin.isUntracked(ksname));

            Iterator var3 = Schema.instance.getTablesAndViews(ksname).iterator();

            while(var3.hasNext()) {
               TableMetadata cfm = (TableMetadata)var3.next();
               this.getThreadPool().submit(() -> {
                  logger.debug("Processing table {}.{}", cfm.keyspace, cfm.name);
                  ColumnFamilyStore cfs = Keyspace.open(cfm.keyspace).getColumnFamilyStore(cfm.name);
                  this.writer.write(TableInfo.fromColumnFamilyStore(cfs));
               });
            }
         }
      } catch (RuntimeException var5) {
         logger.debug("Error performing periodic update of CQL table info", var5);
      }
   }
}
