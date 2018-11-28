package com.datastax.bdp.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import io.reactivex.Completable;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.CassandraAuthorizer;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.GrantMode;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.permission.Permissions;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.AlterTableStatement;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TriggerMetadata;
import org.apache.cassandra.schema.Triggers;
import org.apache.cassandra.schema.ColumnMetadata.Kind;
import org.apache.cassandra.schema.TableMetadata.Builder;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaTool {
   private static Logger logger = LoggerFactory.getLogger(SchemaTool.class);
   private static final Splitter DOT_SPLITTER = Splitter.on(".").trimResults();
   private static final int STABILITY_CHECK_INTERVAL_MS = Integer.getInteger("dse.stability.checkIntervalMs", 10000).intValue();
   private static final int EXTRA_NORMAL_ATTEMPTS = Integer.getInteger("dse.stabilityAttempts", 1).intValue();
   private static final int MAX_ATTEMPTS;
   private static final CountDownLatch stability;

   public SchemaTool() {
   }

   public static KeyspaceMetadata getKeyspaceMetadata(String keyspace) {
      return keyspace == null?null:Schema.instance.getKeyspaceMetadata(keyspace);
   }

   public static TableMetadata getTableMetadata(String keyspace, String table) {
      return keyspace != null?Schema.instance.getTableMetadata(keyspace, table):null;
   }

   public static TableMetadata getCQLTableMetadata(String keyspace, String table) {
      TableMetadata metadata = getTableMetadata(keyspace, table);
      return metadata != null && metadata.isCQLTable()?metadata:null;
   }

   public static ColumnMetadata getColumn(TableMetadata metadata, ByteBuffer columnName) {
      ColumnMetadata column = metadata.getColumn(columnName);
      if(column == null && metadata.isCompactTable()) {
         column = metadata.compactValueColumn;
      }

      if(column == null) {
         throw new IllegalStateException("Could not find column definition for column");
      } else {
         return column;
      }
   }

   public static AbstractType<?> getColumnValidator(TableMetadata metadata, ByteBuffer columnName) {
      ColumnMetadata column = metadata.getColumn(columnName);
      return (AbstractType)(column != null?column.type:(metadata.isCounter()?CounterColumnType.instance:BytesType.instance));
   }

   public static void waitForRingToStabilize() {
      waitForRingToStabilize((String)null);
   }

   public static void waitForRingToStabilize(String keySpace) {
      long start = System.nanoTime();

      try {
         if(stability.getCount() == 0L) {
            return;
         }

         int normalAttempts = 0;
         boolean gossipHasBeenEnabled = false;
         int attempts = 0;

         while(true) {
            if(Gossiper.instance.isEnabled()) {
               if(keySpace != null && getKeyspaceMetadata(keySpace) != null) {
                  stability.await();
                  break;
               }

               gossipHasBeenEnabled = true;
               boolean allNormal = true;
               Iterator var7 = StorageService.instance.getLiveRingMembers(true).iterator();

               while(var7.hasNext()) {
                  InetAddress node = (InetAddress)var7.next();
                  String status = getStatus(node);
                  if(status == null || !status.startsWith("NORMAL")) {
                     allNormal = false;
                     normalAttempts = 0;
                     break;
                  }
               }

               if(allNormal) {
                  if(normalAttempts == EXTRA_NORMAL_ATTEMPTS) {
                     logger.info("All ring nodes are in the NORMAL state now.");
                     break;
                  }

                  ++normalAttempts;
               }

               if(attempts >= MAX_ATTEMPTS) {
                  logger.warn("Ring hasn't stabilized for a long time. continuing");
                  break;
               }

               ++attempts;
            } else {
               if(gossipHasBeenEnabled) {
                  throw new RuntimeException("Server has been shutdown");
               }

               logger.info("Waiting for gossip to start...");
            }

            Thread.sleep((long)STABILITY_CHECK_INTERVAL_MS);
         }
      } catch (InterruptedException var13) {
         throw new RuntimeException(var13);
      } finally {
         if(logger.isDebugEnabled()) {
            logger.debug("Stabilized in {}", Long.valueOf(System.nanoTime() - start));
         }

         stability.countDown();
      }

   }

   private static String getStatus(InetAddress node) {
      EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(node);
      if(state != null) {
         VersionedValue versionedValue = state.getApplicationState(ApplicationState.STATUS);
         if(versionedValue != null) {
            return versionedValue.value;
         }
      }

      return null;
   }

   public static boolean isSchemaAgreement(Map<String, List<String>> schemaVersions) {
      int size = schemaVersions.size();
      if(size == 1) {
         logger.debug("isSchemaAgreement detected only one version; returning true");
         return true;
      } else if(size == 2 && schemaVersions.containsKey("UNREACHABLE")) {
         boolean agreed = true;
         Iterator var3 = ((List)schemaVersions.get("UNREACHABLE")).iterator();

         while(var3.hasNext()) {
            String ip = (String)var3.next();
            EndpointState es = Gossiper.instance.getEndpointStateForEndpoint(DseUtil.getByName(ip));
            boolean isDead = Gossiper.instance.isDeadState(es);
            agreed &= isDead;
            logger.debug("Node {}: isDeadState: {}, EndpointState: {}", new Object[]{ip, Boolean.valueOf(isDead), es});
         }

         logger.debug("isSchemaAgreement returning {}", Boolean.valueOf(agreed));
         return agreed;
      } else {
         logger.debug("isSchemaAgreement returning false; schemaVersions.size(): {}", Integer.valueOf(size));
         return false;
      }
   }

   public static void waitForSchemaAgreement(RetrySetup retrySetup) {
      try {
         DseUtil.getWithRetry(retrySetup, StorageProxy::describeSchemaVersions, SchemaTool::isSchemaAgreement);
      } catch (TimeoutException var2) {
         throw new IllegalStateException("Could not achieve schema agreement", var2);
      }
   }

   public static void maybeDropKeyspace(String name) {
      if(Schema.instance.getKeyspaceMetadata(name) != null) {
         try {
            TPCUtils.blockingAwait(MigrationManager.announceKeyspaceDrop(name));
         } catch (ConfigurationException var2) {
            logger.debug(String.format("Keyspace %s does not exist", new Object[]{name}));
         } catch (Exception var3) {
            throw new AssertionError(var3);
         }
      }

   }

   public static void maybeCreateTrigger(String keyspace, String table, String triggerName, Class<?> triggerClass) {
      maybeCreateTrigger(keyspace, table, triggerName, triggerClass.getName());
   }

   public static void maybeCreateTrigger(String keyspace, String table, String triggerName, String triggerClass) {
      TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
      Triggers triggers = metadata.triggers;
      if(!triggers.get(triggerName).isPresent()) {
         TableMetadata newMetadata = metadata.unbuild().triggers(triggers.with(TriggerMetadata.create(triggerName, triggerClass))).build();
         logger.info("Adding trigger with name {} and class {}", triggerName, triggerClass);
         TPCUtils.blockingAwait(MigrationManager.announceTableUpdate(newMetadata, false));
      }
   }

   public static void maybeDropTrigger(String keyspace, String table, String triggerName) {
      TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
      Triggers triggers = metadata.triggers;
      if(triggers.get(triggerName).isPresent()) {
         TableMetadata newMetadata = metadata.unbuild().triggers(triggers.without(triggerName)).build();
         logger.info("Dropping trigger with name {}", triggerName);
         TPCUtils.blockingAwait(MigrationManager.announceTableUpdate(newMetadata, false));
      }
   }

   public static void maybeCreateOrUpdateKeyspace(KeyspaceMetadata expected, long timestamp) {
      Completable migration;
      if(getKeyspaceMetadata(expected.name) == null) {
         migration = MigrationManager.announceNewKeyspace(expected, timestamp, false).onErrorComplete((e) -> {
            if(e instanceof AlreadyExistsException) {
               logger.debug("Attempted to create new keyspace {}, but it already exists", expected.name);
               return true;
            } else {
               return false;
            }
         });
      } else {
         migration = Completable.complete();
      }

      TPCUtils.blockingAwait(migration.andThen(Completable.defer(() -> {
         KeyspaceMetadata defined = getKeyspaceMetadata(expected.name);
         Preconditions.checkNotNull(defined, String.format("Creating keyspace %s failed", new Object[]{expected.name}));
         List<Completable> migrations = new ArrayList();
         Iterator var3 = expected.types.iterator();

         while(var3.hasNext()) {
            UserType expectedType = (UserType)var3.next();
            UserType definedType = (UserType)defined.types.get(expectedType.name).orElse(null);
            if(definedType == null) {
               migrations.add(MigrationManager.announceNewType(expectedType, false));
            } else if(!expectedType.equals(definedType)) {
               migrations.add(MigrationManager.announceTypeUpdate(expectedType, false));
            }
         }

         var3 = expected.tables.iterator();

         while(true) {
            TableMetadata expectedTable;
            TableMetadata definedTable;
            do {
               if(!var3.hasNext()) {
                  return migrations.isEmpty()?Completable.complete():Completable.merge(migrations);
               }

               expectedTable = (TableMetadata)var3.next();
               definedTable = (TableMetadata)defined.tables.get(expectedTable.name).orElse(null);
            } while(definedTable != null && definedTable.equals(expectedTable));

            migrations.add(MigrationManager.forceAnnounceNewTable(expectedTable));
         }
      })));
   }

   public static void maybeCreateOrUpdateKeyspace(KeyspaceMetadata expected) {
      maybeCreateOrUpdateKeyspace(expected, FBUtilities.timestampMicros());
   }

   public static void maybeAlterKeyspace(KeyspaceMetadata ksm) {
      try {
         TPCUtils.blockingAwait(MigrationManager.announceKeyspaceUpdate(ksm));
      } catch (ConfigurationException var2) {
         logger.debug(String.format("Keyspace %s doesn't exist", new Object[]{ksm.name}));
      } catch (Exception var3) {
         throw new AssertionError(var3);
      }

   }

   public static TableMetadata maybeCreateTable(String keyspace, String table, String cql) {
      TableMetadata metaData = parseCreateTableAndSetDefaults(keyspace, table, cql).build();
      return maybeCreateTable(keyspace, table, metaData);
   }

   public static TableMetadata maybeCreateTable(String keyspace, String table, String cql, String comment) {
      TableMetadata metaData = parseCreateTableAndSetDefaults(keyspace, table, cql).comment(comment).build();
      return maybeCreateTable(keyspace, table, metaData);
   }

   public static Builder parseCreateTableAndSetDefaults(String keyspace, String table, String cql) {
      return CreateTableStatement.parse(String.format(cql, new Object[]{table}), keyspace).id(tableIdForDseSystemTable(keyspace, table)).dcLocalReadRepairChance(0.0D).memtableFlushPeriod((int)TimeUnit.HOURS.toMillis(1L)).gcGraceSeconds((int)TimeUnit.DAYS.toSeconds(14L));
   }

   public static TableMetadata maybeCreateTable(String keyspace, String table, TableMetadata metaData) {
      TableMetadata previousMetaData = Schema.instance.getTableMetadata(keyspace, table);
      if(previousMetaData != null) {
         return previousMetaData;
      } else {
         try {
            TPCUtils.blockingAwait(MigrationManager.announceNewTable(metaData, false));
         } catch (AlreadyExistsException var5) {
            logger.debug(String.format("Table %s.%s already exists", new Object[]{keyspace, table}));
         } catch (Exception var6) {
            throw new AssertionError(var6);
         }

         return metaData;
      }
   }

   public static void maybeDropTable(String keyspace, String table) {
      try {
         TPCUtils.blockingAwait(MigrationManager.announceTableDrop(keyspace, table, false));
      } catch (ConfigurationException var3) {
         logger.debug(String.format("Cannot drop non existing table '%s' in keyspace '%s'.", new Object[]{table, keyspace}));
      } catch (Exception var4) {
         throw new AssertionError(var4);
      }

   }

   public static void maybeAlterTable(String keyspace, String table, String cql) throws InvalidRequestException {
      if(Schema.instance.getTableMetadata(keyspace, table) != null) {
         try {
            CFStatement parsed = (CFStatement)QueryProcessor.parseStatement(cql);
            parsed.prepareKeyspace(keyspace);
            AlterTableStatement statement = (AlterTableStatement)parsed.prepare().statement;
            statement.announceMigration(QueryState.forInternalCalls(), false).blockingGet();
         } catch (InvalidRequestException var5) {
            throw var5;
         } catch (Exception var6) {
            throw new AssertionError(var6);
         }
      }

   }

   public static String getUniqueIndexName(String ks, String cf, String fieldName, Set<String> existingIndexNames) {
      String baseIndexName = ks + "_" + cf + "_" + fieldName.replaceAll("\\W", "") + "_index";
      String indexName = baseIndexName;

      StringBuilder var10000;
      for(int i = 0; existingIndexNames.contains(indexName); indexName = var10000.append(i).toString()) {
         var10000 = (new StringBuilder()).append(baseIndexName).append('_');
         ++i;
      }

      return indexName;
   }

   public static Set<String> existingIndexNames(String keyspaceName, String tableName) {
      Set<String> indexNames = new HashSet();
      TableMetadata table = getTableMetadata(keyspaceName, tableName);
      if(table != null) {
         Iterator var4 = table.indexes.iterator();

         while(var4.hasNext()) {
            IndexMetadata index = (IndexMetadata)var4.next();
            if(StringUtils.isNotEmpty(index.name)) {
               indexNames.add(index.name);
            }
         }
      }

      return indexNames;
   }

   public static boolean cql3KeyspaceExists(String ksName) {
      String describeQuery = String.format("SELECT * FROM %s.%s where keyspace_name = '%s'", new Object[]{"system_schema", "keyspaces", ksName});

      try {
         UntypedResultSet rows = (UntypedResultSet)TPCUtils.blockingGet(QueryProcessor.executeOnceInternal(describeQuery, new Object[0]));
         return rows.size() > 0;
      } catch (Throwable var3) {
         logger.error(var3.getMessage(), var3);
         throw new RuntimeException(var3.getMessage(), var3);
      }
   }

   public static boolean cql3TableExists(String keyspace, String table) {
      return getCQLTableMetadata(keyspace, table) != null;
   }

   public static boolean cql3ColumnExists(String keyspaceName, String tableName, String columnName) {
      TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, tableName);
      return table.getColumn(ByteBufferUtil.bytes(columnName)) != null;
   }

   public static boolean isCql3StaticColumn(String ksName, String cfName, String columnName) {
      TableMetadata cf = Schema.instance.getTableMetadata(ksName, cfName);
      ColumnMetadata def = cf.getColumn(ByteBufferUtil.bytes(columnName));
      return def != null && def.kind.equals(Kind.STATIC);
   }

   public static AbstractType<?> getCql3ColumnType(String keyspace, String table, String column, boolean returnElementType) {
      return getCql3ColumnType(Schema.instance.getTableMetadata(keyspace, table), ByteBufferUtil.bytes(column), returnElementType);
   }

   public static AbstractType<?> getCql3ColumnType(TableMetadata table, ByteBuffer columnName, boolean returnElementType) {
      ColumnMetadata column = table.getColumn(columnName);
      if(column != null) {
         AbstractType<?> validator = column.type;
         return validator.isCollection() && returnElementType?getElementType((CollectionType)validator):validator;
      } else {
         return null;
      }
   }

   public static AbstractType<?> getElementType(CollectionType type) {
      switch (type.kind) {
         case MAP:
         case LIST: {
            return type.valueComparator();
         }
         case SET: {
            return type.nameComparator();
         }
      }
      throw new IllegalStateException("Unexpected collection type: " + (Object)type);
   }

   public static AbstractType<?> getTupleSubFieldType(String solrFieldName, AbstractType<?> parentType, boolean returnElementType) {
      Preconditions.checkArgument(isTupleOrTupleCollection(parentType), "Type '" + parentType + "' is not a tuple or collection of tuples.");
      List<String> columnNameElements = DOT_SPLITTER.splitToList(solrFieldName);
      AbstractType<?> currentType = parentType;

      for(int subFieldCursor = 1; subFieldCursor < columnNameElements.size(); ++subFieldCursor) {
         if(currentType.isCollection() && getElementType((CollectionType)currentType) instanceof TupleType) {
            currentType = getElementType((CollectionType)currentType);
         }

         int typeIndex = -1;
         if(currentType instanceof UserType) {
            typeIndex = ((UserType)currentType).fieldNames().indexOf(FieldIdentifier.forQuoted((String)columnNameElements.get(subFieldCursor)));
         } else if(currentType instanceof TupleType) {
            typeIndex = Integer.parseInt(((String)columnNameElements.get(subFieldCursor)).replaceAll("[^\\d]", "")) - 1;
         }

         if(typeIndex < 0) {
            throw new IllegalStateException("No Cassandra column found: '" + solrFieldName + "'");
         }

         currentType = ((TupleType)currentType).type(typeIndex).asCQL3Type().getType();
      }

      if(currentType.isCollection() && returnElementType) {
         currentType = getElementType((CollectionType)currentType);
      }

      return currentType;
   }

   public static boolean isTupleOrTupleCollection(AbstractType<?> columnType) {
      return columnType instanceof TupleType || columnType.isCollection() && getElementType((CollectionType)columnType) instanceof TupleType;
   }

   public static boolean metadataExists(String ksName, String cfName) {
      return Schema.instance.getTableMetadata(ksName, cfName) != null;
   }

   public static void cql3MaybeGrantUserToTable(String username, String ks, String cf, Permission... permissions) {
      if(DatabaseDescriptor.getAuthorizer().isImplementationOf(CassandraAuthorizer.class)) {
         try {
            cql3GrantUserToKsOrTable(username, String.format("data/%s/%s", new Object[]{ks, cf}), permissions);
         } catch (Exception var5) {
            logger.error(String.format("Failed to authorize user %s to access %s.%s", new Object[]{username, ks, cf}), var5);
         }
      }

   }

   public static void cql3GrantUserToKsOrTable(String username, String dataResource, Permission... permissions) {
      cql3GrantUserToKsOrTable(username, dataResource, (Set)Sets.newHashSet(permissions));
   }

   public static void cql3GrantUserToKsOrTable(String username, String dataResource, Set<Permission> permissions) {
      try {
         DatabaseDescriptor.getAuthorizer().grant(AuthenticatedUser.ANONYMOUS_USER, Permissions.all(), DataResource.fromName(dataResource), RoleResource.role(username), new GrantMode[]{GrantMode.GRANT});
      } catch (RequestExecutionException | RequestValidationException var4) {
         logger.error(var4.getMessage(), var4);
         throw new RuntimeException(var4);
      }
   }

   public static void maybeAddNewColumn(String ks, String table, String columnName, String cql) {
      ColumnMetadata cd = Schema.instance.getTableMetadata(ks, table).getColumn(ByteBufferUtil.bytes(columnName));
      if(null == cd) {
         try {
            maybeAlterTable(ks, table, cql);
         } catch (InvalidRequestException var6) {
            logger.debug(String.format("Caught InvalidRequestException; probably this is just a race with another node attempting to add the column %s.", new Object[]{columnName}), var6);
         }
      }

   }

   public static TableId tableIdForDseSystemTable(String keyspace, String table) {
      return TableId.fromUUID(UUID.nameUUIDFromBytes(ArrayUtils.addAll(keyspace.getBytes(), table.getBytes())));
   }

   static {
      MAX_ATTEMPTS = EXTRA_NORMAL_ATTEMPTS + 71;
      stability = new CountDownLatch(1);
   }
}
