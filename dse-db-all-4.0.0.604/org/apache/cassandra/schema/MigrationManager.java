package org.apache.cassandra.schema;

import io.reactivex.Completable;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrationManager {
   private static final Logger logger = LoggerFactory.getLogger(MigrationManager.class);
   public static final MigrationManager instance = new MigrationManager();
   private static final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
   private static final int MIGRATION_DELAY_IN_MS = Integer.getInteger("cassandra.migration_delay_in_ms", '\uea60').intValue();
   private static final int INITIAL_MIGRATION_DELAY_IN_MS;
   public static final int MIGRATION_TASK_WAIT_IN_SECONDS;

   private MigrationManager() {
   }

   public static void scheduleSchemaPull(InetAddress endpoint, EndpointState state, String reason) {
      UUID schemaVersion = state.getSchemaVersion();
      if(!endpoint.equals(FBUtilities.getBroadcastAddress()) && schemaVersion != null) {
         maybeScheduleSchemaPull(schemaVersion, endpoint, reason);
      }

   }

   private static void maybeScheduleSchemaPull(UUID theirVersion, InetAddress endpoint, String reason) {
      if(Schema.instance.getVersion() == null) {
         logger.debug("Not pulling schema from {}, because local schama version is not known yet", endpoint);
      } else if(Schema.instance.isSameVersion(theirVersion)) {
         logger.debug("Not pulling schema from {}, because schema versions match ({})", endpoint, Schema.schemaVersionToString(theirVersion));
      } else if(!shouldPullSchemaFrom(endpoint)) {
         logger.debug("Not pulling schema from {} due to {}, because shouldPullSchemaFrom returned false", endpoint, reason);
      } else {
         if(!Schema.instance.isEmpty() && runtimeMXBean.getUptime() >= (long)INITIAL_MIGRATION_DELAY_IN_MS) {
            Runnable runnable = () -> {
               UUID epSchemaVersion = Gossiper.instance.getSchemaVersion(endpoint);
               if(epSchemaVersion == null) {
                  logger.debug("epState vanished for {}, not submitting migration task", endpoint);
               } else if(Schema.instance.isSameVersion(epSchemaVersion)) {
                  logger.debug("Not submitting migration task for {} because our versions match ({})", endpoint, epSchemaVersion);
               } else {
                  logger.debug("Submitting migration task for {} due to {}, schema version mismatch: local={}, remote={}", new Object[]{endpoint, reason, Schema.schemaVersionToString(Schema.instance.getVersion()), Schema.schemaVersionToString(epSchemaVersion)});
                  submitMigrationTask(endpoint);
               }
            };
            ScheduledExecutors.nonPeriodicTasks.schedule(runnable, (long)MIGRATION_DELAY_IN_MS, TimeUnit.MILLISECONDS);
         } else {
            logger.debug("Immediately submitting migration task for {}, schema versions: local={}, remote={}", new Object[]{endpoint, reason, Schema.schemaVersionToString(Schema.instance.getVersion()), Schema.schemaVersionToString(theirVersion)});
            submitMigrationTask(endpoint);
         }

      }
   }

   private static Future<?> submitMigrationTask(InetAddress endpoint) {
      return StageManager.getStage(Stage.MIGRATION).submit(new MigrationTask(endpoint));
   }

   static boolean shouldPullSchemaFrom(InetAddress endpoint) {
      return Schema.instance.isSchemaCompatibleWith(endpoint) && !Gossiper.instance.isGossipOnlyMember(endpoint);
   }

   static boolean shouldAnswerPullFrom(InetAddress endpoint) {
      return Schema.instance.isSchemaCompatibleWith(endpoint);
   }

   public static boolean isReadyForBootstrap() {
      return MigrationTask.getInflightTasks().isEmpty();
   }

   public static void waitUntilReadyForBootstrap() {
      logger.info("Waiting until ready to bootstrap ({} timeout)...", Integer.valueOf(MIGRATION_TASK_WAIT_IN_SECONDS));

      CountDownLatch completionLatch;
      while((completionLatch = (CountDownLatch)MigrationTask.getInflightTasks().poll()) != null) {
         try {
            if(!completionLatch.await((long)MIGRATION_TASK_WAIT_IN_SECONDS, TimeUnit.SECONDS)) {
               logger.error("Migration task failed to complete");
            }
         } catch (InterruptedException var2) {
            Thread.currentThread().interrupt();
            logger.error("Migration task was interrupted");
         }
      }

      logger.info("Ready to bootstrap (no more in-flight migration tasks).");
   }

   public static Completable announceNewKeyspace(KeyspaceMetadata ksm) throws ConfigurationException {
      return announceNewKeyspace(ksm, false);
   }

   public static Completable announceNewKeyspace(KeyspaceMetadata ksm, boolean announceLocally) throws ConfigurationException {
      return announceNewKeyspace(ksm, FBUtilities.timestampMicros(), announceLocally);
   }

   public static Completable announceNewKeyspace(KeyspaceMetadata ksm, long timestamp, boolean announceLocally) throws ConfigurationException {
      return Completable.defer(() -> {
         ksm.validate();
         if(Schema.instance.getKeyspaceMetadata(ksm.name) != null) {
            return Completable.error(new AlreadyExistsException(ksm.name));
         } else {
            logger.info("Create new Keyspace: {}", ksm);
            return announce(SchemaKeyspace.makeCreateKeyspaceMutation(ksm, timestamp), announceLocally);
         }
      });
   }

   public static Completable announceNewTable(TableMetadata cfm) throws ConfigurationException {
      return announceNewTable(cfm, false);
   }

   public static Completable announceNewTable(TableMetadata cfm, boolean announceLocally) {
      return announceNewTable(cfm, announceLocally, true);
   }

   public static Completable forceAnnounceNewTable(TableMetadata cfm) {
      return forceAnnounceNewTable(cfm, 0L);
   }

   public static Completable forceAnnounceNewTable(TableMetadata cfm, long timestamp) {
      return announceNewTable(cfm, false, false, timestamp);
   }

   private static Completable announceNewTable(TableMetadata cfm, boolean announceLocally, boolean throwOnDuplicate) {
      return announceNewTable(cfm, announceLocally, throwOnDuplicate, FBUtilities.timestampMicros());
   }

   private static Completable announceNewTable(TableMetadata cfm, boolean announceLocally, boolean throwOnDuplicate, long timestamp) {
      return Completable.defer(() -> {
         cfm.validate();
         KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(cfm.keyspace);
         if(ksm == null) {
            return Completable.error(new ConfigurationException(String.format("Cannot add table '%s' to non existing keyspace '%s'.", new Object[]{cfm.name, cfm.keyspace})));
         } else if(throwOnDuplicate && ksm.getTableOrViewNullable(cfm.name) != null) {
            return Completable.error(new AlreadyExistsException(cfm.keyspace, cfm.name));
         } else {
            logger.info("Create new table: {}", cfm.toDebugString());
            return announce(SchemaKeyspace.makeCreateTableMutation(ksm, cfm, timestamp), announceLocally);
         }
      });
   }

   public static Completable announceNewView(ViewMetadata view, boolean announceLocally) throws ConfigurationException {
      return Completable.defer(() -> {
         view.metadata.validate();
         KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(view.keyspace);
         if(ksm == null) {
            return Completable.error(new ConfigurationException(String.format("Cannot add table '%s' to non existing keyspace '%s'.", new Object[]{view.name, view.keyspace})));
         } else if(ksm.getTableOrViewNullable(view.name) != null) {
            return Completable.error(new AlreadyExistsException(view.keyspace, view.name));
         } else {
            logger.info("Create new view: {}", view);
            return announce(SchemaKeyspace.makeCreateViewMutation(ksm, view, FBUtilities.timestampMicros()), announceLocally);
         }
      });
   }

   public static Completable announceNewType(UserType newType, boolean announceLocally) {
      return announceNewType(newType, announceLocally, FBUtilities.timestampMicros());
   }

   public static Completable forceAnnounceNewType(UserType newType) {
      return forceAnnounceNewType(newType, 0L);
   }

   public static Completable forceAnnounceNewType(UserType newType, long timestamp) {
      return announceNewType(newType, false, timestamp);
   }

   public static Completable announceNewType(UserType newType, boolean announceLocally, long timestamp) {
      KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(newType.keyspace);
      return announce(SchemaKeyspace.makeCreateTypeMutation(ksm, newType, timestamp), announceLocally);
   }

   public static Completable announceNewFunction(UDFunction udf, boolean announceLocally) {
      logger.info("Create scalar function '{}'", udf.name());
      KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(udf.name().keyspace);
      return announce(SchemaKeyspace.makeCreateFunctionMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
   }

   public static Completable announceNewAggregate(UDAggregate udf, boolean announceLocally) {
      logger.info("Create aggregate function '{}'", udf.name());
      KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(udf.name().keyspace);
      return announce(SchemaKeyspace.makeCreateAggregateMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
   }

   public static Completable announceKeyspaceUpdate(KeyspaceMetadata ksm) throws ConfigurationException {
      return announceKeyspaceUpdate(ksm, false);
   }

   public static Completable announceKeyspaceUpdate(KeyspaceMetadata ksm, boolean announceLocally) throws ConfigurationException {
      return Completable.defer(() -> {
         ksm.validate();
         KeyspaceMetadata oldKsm = Schema.instance.getKeyspaceMetadata(ksm.name);
         if(oldKsm == null) {
            return Completable.error(new ConfigurationException(String.format("Cannot update non existing keyspace '%s'.", new Object[]{ksm.name})));
         } else {
            logger.info("Update Keyspace '{}' From {} To {}", new Object[]{ksm.name, oldKsm, ksm});
            return announce(SchemaKeyspace.makeCreateKeyspaceMutation(ksm.name, ksm.params, FBUtilities.timestampMicros()), announceLocally);
         }
      });
   }

   public static Completable announceTableUpdate(TableMetadata tm) throws ConfigurationException {
      return announceTableUpdate(tm, false);
   }

   public static Completable announceTableUpdate(TableMetadata updated, boolean announceLocally) throws ConfigurationException {
      return announceTableUpdate(updated, (Collection)null, announceLocally);
   }

   public static Completable announceTableUpdate(TableMetadata updated, Collection<ViewMetadata> views, boolean announceLocally) throws ConfigurationException {
      return Completable.defer(() -> {
         updated.validate();
         TableMetadata current = Schema.instance.getTableMetadata(updated.keyspace, updated.name);
         if(current == null) {
            return Completable.error(new ConfigurationException(String.format("Cannot update non existing table '%s' in keyspace '%s'.", new Object[]{updated.name, updated.keyspace})));
         } else {
            KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(current.keyspace);
            current.validateCompatibility(updated);
            long timestamp = FBUtilities.timestampMicros();
            logger.info("Update table '{}/{}' From {} To {}", new Object[]{current.keyspace, current.name, current.toDebugString(), updated.toDebugString()});
            Mutation.SimpleBuilder builder = SchemaKeyspace.makeUpdateTableMutation(ksm, current, updated, timestamp);
            if(views != null) {
               views.forEach((view) -> {
                  addViewUpdateToMutationBuilder(view, builder);
               });
            }

            return announce(builder, announceLocally);
         }
      });
   }

   public static Completable announceViewUpdate(ViewMetadata view, boolean announceLocally) throws ConfigurationException {
      return Completable.defer(() -> {
         KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(view.keyspace);
         long timestamp = FBUtilities.timestampMicros();
         Mutation.SimpleBuilder builder = SchemaKeyspace.makeCreateKeyspaceMutation(ksm.name, ksm.params, timestamp);
         addViewUpdateToMutationBuilder(view, builder);
         return announce(builder, announceLocally);
      });
   }

   private static void addViewUpdateToMutationBuilder(ViewMetadata view, Mutation.SimpleBuilder builder) {
      view.metadata.validate();
      ViewMetadata oldView = Schema.instance.getView(view.keyspace, view.name);
      if(oldView == null) {
         throw new ConfigurationException(String.format("Cannot update non existing materialized view '%s' in keyspace '%s'.", new Object[]{view.name, view.keyspace}));
      } else {
         oldView.metadata.validateCompatibility(view.metadata);
         logger.info("Update view '{}/{}' From {} To {}", new Object[]{view.keyspace, view.name, oldView, view});
         SchemaKeyspace.makeUpdateViewMutation(builder, oldView, view);
      }
   }

   public static Completable announceTypeUpdate(UserType updatedType, boolean announceLocally) {
      logger.info("Update type '{}.{}' to {}", new Object[]{updatedType.keyspace, updatedType.getNameAsString(), updatedType});
      return announceNewType(updatedType, announceLocally);
   }

   public static Completable announceKeyspaceDrop(String ksName) throws ConfigurationException {
      return announceKeyspaceDrop(ksName, false);
   }

   public static Completable announceKeyspaceDrop(String ksName, boolean announceLocally) throws ConfigurationException {
      return Completable.defer(() -> {
         KeyspaceMetadata oldKsm = Schema.instance.getKeyspaceMetadata(ksName);
         if(oldKsm == null) {
            return Completable.error(new ConfigurationException(String.format("Cannot drop non existing keyspace '%s'.", new Object[]{ksName})));
         } else {
            logger.info("Drop Keyspace '{}'", oldKsm.name);
            return announce(SchemaKeyspace.makeDropKeyspaceMutation(oldKsm, FBUtilities.timestampMicros()), announceLocally);
         }
      });
   }

   public static Completable announceTableDrop(String ksName, String cfName) throws ConfigurationException {
      return announceTableDrop(ksName, cfName, false);
   }

   public static Completable announceTableDrop(String ksName, String cfName, boolean announceLocally) throws ConfigurationException {
      return Completable.defer(() -> {
         TableMetadata tm = Schema.instance.getTableMetadata(ksName, cfName);
         if(tm == null) {
            return Completable.error(new ConfigurationException(String.format("Cannot drop non existing table '%s' in keyspace '%s'.", new Object[]{cfName, ksName})));
         } else {
            KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(ksName);
            logger.info("Drop table '{}/{}'", tm.keyspace, tm.name);
            return announce(SchemaKeyspace.makeDropTableMutation(ksm, tm, FBUtilities.timestampMicros()), announceLocally);
         }
      });
   }

   public static Completable announceViewDrop(String ksName, String viewName, boolean announceLocally) throws ConfigurationException {
      return Completable.defer(() -> {
         ViewMetadata view = Schema.instance.getView(ksName, viewName);
         if(view == null) {
            return Completable.error(new ConfigurationException(String.format("Cannot drop non existing materialized view '%s' in keyspace '%s'.", new Object[]{viewName, ksName})));
         } else {
            KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(ksName);
            logger.info("Drop table '{}/{}'", view.keyspace, view.name);
            return announce(SchemaKeyspace.makeDropViewMutation(ksm, view, FBUtilities.timestampMicros()), announceLocally);
         }
      });
   }

   public static Completable announceTypeDrop(UserType droppedType, boolean announceLocally) {
      KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(droppedType.keyspace);
      return announce(SchemaKeyspace.dropTypeFromSchemaMutation(ksm, droppedType, FBUtilities.timestampMicros()), announceLocally);
   }

   public static Completable announceFunctionDrop(UDFunction udf, boolean announceLocally) {
      logger.info("Drop scalar function overload '{}' args '{}'", udf.name(), udf.argTypes());
      KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(udf.name().keyspace);
      return announce(SchemaKeyspace.makeDropFunctionMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
   }

   public static Completable announceAggregateDrop(UDAggregate udf, boolean announceLocally) {
      logger.info("Drop aggregate function overload '{}' args '{}'", udf.name(), udf.argTypes());
      KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(udf.name().keyspace);
      return announce(SchemaKeyspace.makeDropAggregateMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
   }

   private static Completable announce(Mutation.SimpleBuilder schema, boolean announceLocally) {
      SchemaMigration migration = SchemaMigration.schema(Collections.singletonList(schema.build()));
      if(announceLocally) {
         Completable migrationCompletable = Completable.fromRunnable(() -> {
            Schema.instance.merge(migration);
         });
         if(TPC.isTPCThread()) {
            migrationCompletable = migrationCompletable.subscribeOn(StageManager.getScheduler(Stage.MIGRATION));
         }

         return migrationCompletable;
      } else {
         return announce(migration);
      }
   }

   private static void pushSchemaMutation(InetAddress endpoint, SchemaMigration schema) {
      logger.debug("Pushing schema to endpoint {}", endpoint);
      MessagingService.instance().send(Verbs.SCHEMA.PUSH.newRequest(endpoint, schema));
   }

   private static boolean canPushToEndpoint(InetAddress endpoint) {
      return !endpoint.equals(FBUtilities.getBroadcastAddress()) && Schema.instance.isSchemaCompatibleWith(endpoint);
   }

   private static Completable announce(SchemaMigration schema) {
      Completable migration = Completable.fromRunnable(() -> {
         Schema.instance.mergeAndAnnounceVersion(schema);
         Iterator var1 = Gossiper.instance.getLiveMembers().iterator();

         while(var1.hasNext()) {
            InetAddress endpoint = (InetAddress)var1.next();
            if(canPushToEndpoint(endpoint)) {
               pushSchemaMutation(endpoint, schema);
            }
         }

      });
      if(TPC.isTPCThread()) {
         migration = migration.subscribeOn(StageManager.getScheduler(Stage.MIGRATION));
      }

      return migration;
   }

   static void passiveAnnounce(UUID version) {
      Gossiper.instance.addLocalApplicationState(ApplicationState.SCHEMA, StorageService.instance.valueFactory.schema(version));
      logger.debug("Gossiping my schema version {}", version);
   }

   public static void resetLocalSchema() {
      logger.info("Starting local schema reset...");
      logger.debug("Truncating schema tables...");
      SchemaKeyspace.truncate();
      logger.debug("Clearing local schema keyspace definitions...");
      Schema.instance.clear();
      Set<InetAddress> liveEndpoints = Gossiper.instance.getLiveMembers();
      liveEndpoints.remove(FBUtilities.getBroadcastAddress());
      Iterator var1 = liveEndpoints.iterator();

      while(var1.hasNext()) {
         InetAddress node = (InetAddress)var1.next();
         if(shouldPullSchemaFrom(node)) {
            logger.debug("Requesting schema from {}", node);
            FBUtilities.waitOnFuture(submitMigrationTask(node));
            break;
         }
      }

      logger.info("Local schema reset is complete.");
   }

   static {
      INITIAL_MIGRATION_DELAY_IN_MS = Integer.getInteger("cassandra.initial_migration_delay_in_ms", MIGRATION_DELAY_IN_MS).intValue();
      MIGRATION_TASK_WAIT_IN_SECONDS = Integer.getInteger("cassandra.migration_task_wait_in_seconds", 1).intValue();
   }
}
