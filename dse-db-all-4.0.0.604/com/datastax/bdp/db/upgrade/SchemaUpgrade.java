package com.datastax.bdp.db.upgrade;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class SchemaUpgrade {
   private final KeyspaceMetadata keyspaceMetadata;
   private final List<TableMetadata> tables;
   private final boolean schemaChangeUseCurrentTimestamp;

   public SchemaUpgrade(KeyspaceMetadata keyspaceMetadata, List<TableMetadata> tables, boolean schemaChangeUseCurrentTimestamp) {
      this.keyspaceMetadata = keyspaceMetadata;
      this.tables = tables;
      this.schemaChangeUseCurrentTimestamp = schemaChangeUseCurrentTimestamp;
   }

   public boolean ddlChangeRequired() {
      KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(this.keyspaceMetadata.name);
      if(ksm == null) {
         return true;
      } else {
         Tables t = this.tablesWithUpdates(ksm);
         return t != ksm.tables;
      }
   }

   public void executeDDL() {
      KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(this.keyspaceMetadata.name);
      if(ksm == null) {
         ksm = this.keyspaceMetadata;
      }

      Tables t = this.tablesWithUpdates(ksm);
      if(t != ksm.tables) {
         ksm = ksm.withSwapped(t);
         TPCUtils.blockingAwait(StorageService.instance.maybeAddOrUpdateKeyspace(ksm, Collections.emptyList(), this.schemaModificationTimestamp()));
      }
   }

   private long schemaModificationTimestamp() {
      return this.schemaChangeUseCurrentTimestamp?FBUtilities.timestampMicros():0L;
   }

   private Tables tablesWithUpdates(KeyspaceMetadata ksm) {
      Tables t = ksm.tables;

      TableMetadata table;
      for(Iterator var3 = this.tables.iterator(); var3.hasNext(); t = this.withMissingColumns(t, table)) {
         table = (TableMetadata)var3.next();
      }

      return t;
   }

   private Tables withMissingColumns(Tables current, TableMetadata expected) {
      TableMetadata existing = current.getNullable(expected.name);
      if(existing == null) {
         return current.with(expected);
      } else if(existing.partitionKeyColumns().equals(expected.partitionKeyColumns()) && existing.clusteringColumns().equals(expected.clusteringColumns())) {
         TableMetadata.Builder builder = null;
         Iterator var5 = expected.regularAndStaticColumns().iterator();

         while(var5.hasNext()) {
            ColumnMetadata expectedColumn = (ColumnMetadata)var5.next();
            if(existing.getColumn(expectedColumn.name) == null) {
               if(builder == null) {
                  builder = existing.unbuild();
               }

               if(expectedColumn.isStatic()) {
                  builder.addStaticColumn(expectedColumn.name, expectedColumn.type);
               } else {
                  builder.addRegularColumn(expectedColumn.name, expectedColumn.type);
               }
            }
         }

         return builder != null?current.withSwapped(builder.build()):current;
      } else {
         throw new RuntimeException(String.format("The primary key columns of the existing table table definition and the expected table definition for %s.%s do not match", new Object[]{existing.keyspace, existing.name}));
      }
   }
}
