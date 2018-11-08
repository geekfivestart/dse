package com.datastax.bdp.cassandra.auth;

import com.datastax.bdp.cassandra.cql3.RLACExpression;
import com.datastax.bdp.cassandra.cql3.RlacWhitelistStatement;
import com.datastax.bdp.cassandra.cql3.RpcCallStatement;
import com.google.inject.Inject;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.PermissionSets;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLStatementUtils;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.statements.AuthenticationStatement;
import org.apache.cassandra.cql3.statements.AuthorizationStatement;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SchemaAlteringStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.TruncateStatement;
import org.apache.cassandra.cql3.statements.UseStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowLevelAccessControlAuthorizer {
   private static final Logger logger = LoggerFactory.getLogger(RowLevelAccessControlAuthorizer.class);
   public static final String EXTENSION_KEY = "DSE_RLACA";
   private static final PermissionSets GRANTED_SELECT;
   private static final PermissionSets GRANTED_MODIFY;
   private static final PermissionSets GRANTED_MODIFY_AND_SELECT;
   @Inject(
      optional = true
   )
   private static Set<RowLevelAccessControlComponentAuthorizer> rowLevelAccessControlComponentAuthorizers;

   public RowLevelAccessControlAuthorizer() {
   }

   public static Set<RowLevelAccessControlComponentAuthorizer> getRowLevelAccessControlComponentAuthorizers() {
      return rowLevelAccessControlComponentAuthorizers;
   }

   public static CQLStatement approveStatement(CQLStatement statement, QueryState state, QueryOptions options) {
      if(!isEnabled()) {
         return statement;
      } else {
         if(statement instanceof BatchStatement) {
            ((BatchStatement)statement).getStatements().forEach((ms) -> {
               verifyModificationPermissions(ms, state, options);
            });
         } else {
            if(statement instanceof SelectStatement) {
               return verifyAndMaybeUpdateSelect((SelectStatement)statement, state, options);
            }

            if(statement instanceof ModificationStatement) {
               verifyModificationPermissions((ModificationStatement)statement, state, options);
            } else if(statement instanceof TruncateStatement) {
               String keyspace = CQLStatementUtils.getKeyspace(statement);
               String table = CQLStatementUtils.getTable(statement);
               verifyUserHasPermissionForStatement(state, keyspace, table, CorePermission.MODIFY);
            } else if(!(statement instanceof UseStatement) && !(statement instanceof AuthenticationStatement) && !(statement instanceof AuthorizationStatement) && !(statement instanceof RpcCallStatement) && !(statement instanceof SchemaAlteringStatement) && !(statement instanceof RlacWhitelistStatement)) {
               throw new RuntimeException(String.format("Row Level Access Controller Does Not Know How To Handle Statement %s", new Object[]{statement}));
            }
         }

         return statement;
      }
   }

   public static void approveBatchStatement(BatchStatement statement, QueryState state, BatchQueryOptions options) {
      if(isEnabled()) {
         for(int i = 0; i < statement.getStatements().size(); ++i) {
            approveStatement((CQLStatement)statement.getStatements().get(i), state, options.forStatement(i));
         }
      }

   }

   public static boolean isEnabled() {
      IAuthorizer authorizer = DatabaseDescriptor.getAuthorizer();
      return authorizer.isImplementationOf(DseAuthorizer.class) && ((DseAuthorizer)authorizer.implementation()).isRowLevelEnabled() && authorizer.requireAuthorization();
   }

   private static boolean shouldShortCircuitRlac(QueryState state) {
      AuthenticatedUser user = state.getUser();
      return user == null || user.isAnonymous() || user.isSystem() || state.isSuper();
   }

   private static boolean shouldShortCircuitRlacOrNoTarget(QueryState state, String rlacTarget) {
      return shouldShortCircuitRlac(state) || rlacTarget == null;
   }

   private static boolean userHasPermissionToTableOrKs(QueryState state, Permission permission, String table, String ks) {
      return state.hasPermission(table != null?DataResource.table(ks, table):DataResource.keyspace(ks), permission);
   }

   private static SelectStatement verifyAndMaybeUpdateSelect(SelectStatement select, QueryState state, QueryOptions options) {
      String rlacTarget = findRlacTargetColumn(select.table);
      AuthenticatedUser user = state.getClientState().getUser();
      if(shouldShortCircuitRlacOrNoTarget(state, rlacTarget)) {
         return select;
      } else if(userHasPermissionToTableOrKs(state, CorePermission.SELECT, select.columnFamily(), select.keyspace())) {
         return select;
      } else {
         boolean authorized = verifyRestrictions(select.getRestrictions(), select.table, options, state, true);
         if(!authorized) {
            throw new UnauthorizedException("Not authorized");
         } else {
            UserRolesAndPermissions userRolesAndPermissions = state.getUserRolesAndPermissions();
            userRolesAndPermissions.additionalQueryPermission(DataResource.table(select.keyspace(), select.columnFamily()), GRANTED_SELECT);
            if(select.table.isView()) {
               TableMetadataRef baseTable = View.findBaseTable(select.keyspace(), select.columnFamily());
               if(baseTable != null) {
                  userRolesAndPermissions.additionalQueryPermission(DataResource.table(baseTable.keyspace, baseTable.name), GRANTED_SELECT);
               }
            }

            logger.debug("Restricting select for user {}", user);
            RLACExpression expression = RLACExpression.newExpression(select.table, state);
            return select.addIndexRestrictions(Collections.singletonList(expression));
         }
      }
   }

   private static void verifyModificationPermissions(ModificationStatement modification, QueryState state, QueryOptions options) {
      String rlacTarget = findRlacTargetColumn(modification.metadata());
      if(!shouldShortCircuitRlacOrNoTarget(state, rlacTarget)) {
         StatementRestrictions restrictions;
         UserRolesAndPermissions userRolesAndPermissions;
         if(userHasPermissionToTableOrKs(state, CorePermission.MODIFY, modification.columnFamily(), modification.keyspace())) {
            if(modification.hasConditions() && !userHasPermissionToTableOrKs(state, CorePermission.SELECT, modification.columnFamily(), modification.keyspace())) {
               restrictions = modification.getRestrictions();
               if(!verifyRestrictions(restrictions, modification.metadata(), options, state, true)) {
                  throw new UnauthorizedException("Not authorized");
               }

               userRolesAndPermissions = state.getUserRolesAndPermissions();
               userRolesAndPermissions.additionalQueryPermission(DataResource.table(modification.keyspace(), modification.columnFamily()), GRANTED_SELECT);
            }
         } else {
            restrictions = modification.getRestrictions();
            if(!verifyRestrictions(restrictions, modification.metadata(), options, state, false)) {
               throw new UnauthorizedException("Not authorized");
            }

            if(modification.hasConditions()) {
               if(userHasPermissionToTableOrKs(state, CorePermission.SELECT, modification.columnFamily(), modification.keyspace())) {
                  userRolesAndPermissions = state.getUserRolesAndPermissions();
                  userRolesAndPermissions.additionalQueryPermission(DataResource.table(modification.keyspace(), modification.columnFamily()), GRANTED_MODIFY);
               } else {
                  if(!verifyRestrictions(restrictions, modification.metadata(), options, state, true)) {
                     throw new UnauthorizedException("Not authorized");
                  }

                  userRolesAndPermissions = state.getUserRolesAndPermissions();
                  userRolesAndPermissions.additionalQueryPermission(DataResource.table(modification.keyspace(), modification.columnFamily()), GRANTED_MODIFY_AND_SELECT);
               }
            } else {
               userRolesAndPermissions = state.getUserRolesAndPermissions();
               userRolesAndPermissions.additionalQueryPermission(DataResource.table(modification.keyspace(), modification.columnFamily()), GRANTED_MODIFY);
            }
         }

      }
   }

   private static void verifyUserHasPermissionForStatement(QueryState state, String keyspace, String table, Permission permission) {
      if(!shouldShortCircuitRlac(state)) {
         if(!userHasPermissionToTableOrKs(state, permission, table, keyspace)) {
            throw new UnauthorizedException("Not authorized");
         }
      }
   }

   public static boolean authorizeLocalRead(QueryState state, TableMetadata cfm, DecoratedKey key, Row row) {
      ByteBuffer extension = null;
      if(cfm.params.extensions != null) {
         extension = (ByteBuffer)cfm.params.extensions.get("DSE_RLACA");
      }

      ColumnMetadata tenantColumn = cfm.getColumn(extension);

      assert tenantColumn != null && tenantColumn.isPrimaryKeyColumn();

      Set<String> rowTargets = findRowTargetsForUser(cfm, state, CorePermission.SELECT);
      return tenantColumn.isPartitionKey()?authorizeByPartitionKey(cfm.partitionKeyType, tenantColumn, key.getKey(), rowTargets):authorizeByClusteringColumn(row.clustering(), tenantColumn, rowTargets);
   }

   private static boolean verifyRestrictions(StatementRestrictions restrictions, TableMetadata cfm, QueryOptions options, QueryState state, boolean isForRead) {
      Permission permission = isForRead?CorePermission.SELECT:CorePermission.MODIFY;
      if(restrictions.isKeyRange()) {
         return isForRead?!findRowTargetsForUser(cfm, state, permission).isEmpty():false;
      } else {
         ByteBuffer extension = null;
         if(cfm.params.extensions != null) {
            extension = (ByteBuffer)cfm.params.extensions.get("DSE_RLACA");
         }

         ColumnMetadata tenantColumn = cfm.getColumn(extension);

         assert tenantColumn != null && tenantColumn.isPrimaryKeyColumn();

         Set<String> rowTargets = findRowTargetsForUser(cfm, state, permission);
         if(tenantColumn.isPartitionKey()) {
            AbstractType<?> keyType = cfm.partitionKeyType;
            List<ByteBuffer> partitionKeys = restrictions.getPartitionKeys(options);
            Iterator var11 = partitionKeys.iterator();

            while(var11.hasNext()) {
               ByteBuffer key = (ByteBuffer)var11.next();
               if(key.hasRemaining() && !authorizeByPartitionKey(keyType, tenantColumn, key, rowTargets)) {
                  return false;
               }
            }
         } else if(tenantColumn.isClusteringColumn()) {
            Iterator var9 = restrictions.getClusteringColumns(options).iterator();

            while(var9.hasNext()) {
               Clustering clustering = (Clustering)var9.next();
               if(!isForRead) {
                  if(clustering.size() <= tenantColumn.position() || clustering.get(tenantColumn.position()) == null || !authorizeByClusteringColumn(clustering, tenantColumn, rowTargets)) {
                     return false;
                  }
               } else if(clustering.size() > tenantColumn.position() && !authorizeByClusteringColumn(clustering, tenantColumn, rowTargets)) {
                  return false;
               }
            }
         }

         return true;
      }
   }

   private static Set<String> findRowTargetsForUser(TableMetadata cfm, QueryState state, Permission permission) {
      DseAuthorizer authorizer = (DseAuthorizer)DatabaseDescriptor.getAuthorizer().implementation();
      return authorizer.findRowTargetsForUser(state, cfm.resource, permission);
   }

   private static boolean authorizeByPartitionKey(AbstractType<?> keyType, ColumnMetadata tenantColumn, ByteBuffer key, Set<String> rowTargets) {
      ByteBuffer partitionTenant = keyType instanceof CompositeType?CompositeType.extractComponent(key, tenantColumn.position()):key;
      String rowTenantString = tenantColumn.type.getString(partitionTenant);
      return rowTargets.contains(rowTenantString);
   }

   private static boolean authorizeByClusteringColumn(Clustering clustering, ColumnMetadata tenantColumn, Set<String> rowTargets) {
      if(clustering == Clustering.STATIC_CLUSTERING) {
         return true;
      } else {
         ByteBuffer rowTenant = clustering.get(tenantColumn.position());
         String rowTenantString = tenantColumn.type.getString(rowTenant);
         return rowTargets.contains(rowTenantString);
      }
   }

   public static String findRlacTargetColumn(String keyspace, String table) {
      if(keyspace != null && Schema.instance.getKeyspaceInstance(keyspace) != null) {
         KeyspaceMetadata ksm = Schema.instance.getKeyspaceInstance(keyspace).getMetadata();
         return table != null && ksm.getTableOrViewNullable(table) != null?findRlacTargetColumn(ksm.getTableOrViewNullable(table)):null;
      } else {
         return null;
      }
   }

   static String findRlacTargetColumn(TableMetadata cfm) {
      String rlacTarget = null;
      if(cfm != null) {
         try {
            if(cfm.params.extensions != null) {
               ByteBuffer extension = (ByteBuffer)cfm.params.extensions.get("DSE_RLACA");
               if(extension != null) {
                  rlacTarget = ByteBufferUtil.string(extension);
               }
            }

            logger.debug("We found the rlacTarget to be: {}", rlacTarget);
         } catch (CharacterCodingException var3) {
            ;
         }
      }

      return rlacTarget;
   }

   static {
      GRANTED_SELECT = PermissionSets.builder().addGranted(CorePermission.SELECT).build();
      GRANTED_MODIFY = PermissionSets.builder().addGranted(CorePermission.MODIFY).build();
      GRANTED_MODIFY_AND_SELECT = PermissionSets.builder().addGranted(CorePermission.MODIFY).addGranted(CorePermission.SELECT).build();
   }
}
