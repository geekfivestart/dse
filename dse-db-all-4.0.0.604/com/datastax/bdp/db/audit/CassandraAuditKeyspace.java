package com.datastax.bdp.db.audit;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang3.ArrayUtils;

public class CassandraAuditKeyspace {
   public static final String NAME = "dse_audit";
   public static final String AUDIT_LOG = "audit_log";
   private static final TableMetadata AuditLog = compile("dse_audit", "audit_log", "CREATE TABLE IF NOT EXISTS %s.%s (date timestamp,node inet,day_partition int,event_time timeuuid,batch_id uuid,category text,keyspace_name text,operation text,source text,table_name text,type text,username text,authenticated text,consistency text,PRIMARY KEY ((date, node, day_partition), event_time)) WITH COMPACTION={'class':'TimeWindowCompactionStrategy'}");
   public static final KeyspaceMetadata metadata = KeyspaceMetadata.create("dse_audit", KeyspaceParams.simple(1), Tables.of(new TableMetadata[0]));

   public CassandraAuditKeyspace() {
   }

   public static KeyspaceMetadata metadata() {
      return metadata;
   }

   public static List<TableMetadata> tablesIfNotExist() {
      return Collections.singletonList(AuditLog);
   }

   public static void maybeConfigure() {
      TPCUtils.blockingAwait(StorageService.instance.maybeAddOrUpdateKeyspace(metadata(), tablesIfNotExist(), FBUtilities.timestampMicros()));
   }

   private static TableMetadata compile(String keyspaceName, String tableName, String schema) {
      return CreateTableStatement.parse(String.format(schema, new Object[]{keyspaceName, tableName}), keyspaceName).id(tableId(keyspaceName, tableName)).dcLocalReadRepairChance(0.0D).memtableFlushPeriod((int)TimeUnit.HOURS.toMillis(1L)).gcGraceSeconds((int)TimeUnit.DAYS.toSeconds(90L)).build();
   }

   static TableId tableId(String keyspace, String table) {
      byte[] bytes = ArrayUtils.addAll(keyspace.getBytes(), table.getBytes());
      return TableId.fromUUID(UUID.nameUUIDFromBytes(bytes));
   }

   public static Keyspace getKeyspace() {
      return Keyspace.open("dse_audit");
   }

   public static IPartitioner getAuditLogPartitioner() {
      return AuditLog.partitioner;
   }
}
