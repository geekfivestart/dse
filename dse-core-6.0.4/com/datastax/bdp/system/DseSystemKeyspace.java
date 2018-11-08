package com.datastax.bdp.system;

import com.datastax.bdp.util.SchemaTool;
import com.datastax.bdp.util.SyncRepairRunner;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.EverywhereStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DseSystemKeyspace {
   public static final String NAME = "dse_system";
   public static final String ENCRYPTED_KEYS = "encrypted_keys";
   public static final TableMetadata EncryptedKeys = compile("encrypted_keys", "CREATE TABLE %s.%s (key_file TEXT, cipher TEXT, strength INT, key_id TIMEUUID, key TEXT, PRIMARY KEY (key_file, cipher, strength, key_id))");
   private static Logger logger = LoggerFactory.getLogger(DseSystemKeyspace.class);

   private DseSystemKeyspace() {
   }

   public static TableMetadata compile(String keyspaceName, String tableName, String schema) {
      return CreateTableStatement.parse(String.format(schema, new Object[]{keyspaceName, tableName}), keyspaceName).id(SchemaTool.tableIdForDseSystemTable(keyspaceName, tableName)).dcLocalReadRepairChance(0.0D).memtableFlushPeriod((int)TimeUnit.HOURS.toMillis(1L)).gcGraceSeconds((int)TimeUnit.DAYS.toSeconds(90L)).build();
   }

   public static TableMetadata compile(String tableName, String schema) {
      return compile("dse_system", tableName, schema);
   }

   private static Tables tables() {
      return Tables.of(new TableMetadata[]{EncryptedKeys});
   }

   public static void maybeConfigure() {
      try {
         KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata("dse_system");
         KeyspaceMetadata metadata = metadata();
         if(ksm == null) {
            SchemaTool.maybeCreateOrUpdateKeyspace(metadata);
         } else if(!ksm.params.replication.klass.equals(EverywhereStrategy.class)) {
            SchemaTool.maybeAlterKeyspace(metadata);
            if(Gossiper.instance.getUnreachableMembers().isEmpty()) {
               repairDseSystemKeyspace(ksm);
            } else {
               logger.error(String.format("Replication strategy class for %1$s keyspace changed to EverywhereStrategy, and some nodes are down, so we can't proceed with repair now. Please run 'nodetool repair %1$s' once all nodes are up.", new Object[]{"dse_system"}));
            }
         }

      } catch (IOException var2) {
         throw new RuntimeException(var2);
      }
   }

   private static void repairDseSystemKeyspace(KeyspaceMetadata ksm) throws IOException {
      Set<String> tableSet = new HashSet();
      ksm.tables.forEach((table) -> {
         tableSet.add(table.name);
      });
      String[] tableArray = (String[])tableSet.toArray(new String[tableSet.size()]);
      SyncRepairRunner.repair("dse_system", tableArray);
   }

   private static KeyspaceMetadata metadata() {
      return KeyspaceMetadata.create("dse_system", KeyspaceParams.create(true, ImmutableMap.of("class", EverywhereStrategy.class.getCanonicalName())), tables());
   }

   public static boolean isCreated() {
      return Schema.instance.getKeyspaceMetadata("dse_system") != null;
   }
}
