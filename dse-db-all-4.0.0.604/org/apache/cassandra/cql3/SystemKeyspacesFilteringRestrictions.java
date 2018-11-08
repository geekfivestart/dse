package org.apache.cassandra.cql3;

import io.reactivex.Maybe;
import io.reactivex.functions.Function;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.Resources;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.restrictions.AuthRestriction;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.flow.Flow;

public final class SystemKeyspacesFilteringRestrictions {
   public SystemKeyspacesFilteringRestrictions() {
   }

   private static boolean enabled() {
      return DatabaseDescriptor.isSystemKeyspaceFilteringEnabled();
   }

   public static void hasMandatoryPermissions(String keyspace, QueryState state) {
      if(enabled() && state.isOrdinaryUser() && SchemaConstants.isUserKeyspace(keyspace)) {
         state.checkPermission(DataResource.keyspace(keyspace), CorePermission.DESCRIBE);
      }

   }

   private static boolean checkDescribePermissionOnKeyspace(String keyspace, UserRolesAndPermissions userRolesAndPermissions) {
      return userRolesAndPermissions.hasPermission(DataResource.keyspace(keyspace), CorePermission.DESCRIBE);
   }

   public static Server.ChannelFilter getChannelFilter(Event.SchemaChange event) {
      String keyspace = event.keyspace;
      return enabled() && !SchemaConstants.isLocalSystemKeyspace(keyspace) && !SchemaConstants.isReplicatedSystemKeyspace(keyspace)?(channel) -> {
         ClientState clientState = (ClientState)channel.attr(Server.ATTR_KEY_CLIENT_STATE).get();
         AuthenticatedUser user = clientState.getUser();
         return DatabaseDescriptor.getAuthManager().getUserRolesAndPermissions(user).flatMapMaybe((userRolesAndPermissions) -> {
            return checkDescribePermissionOnKeyspace(keyspace, userRolesAndPermissions)?Maybe.just(channel):Maybe.never();
         });
      }:Server.ChannelFilter.NOOP_FILTER;
   }

   public static AuthRestriction restrictionsForTable(TableMetadata tableMetadata) {
      if(!enabled()) {
         return null;
      } else {
         String var1 = tableMetadata.keyspace;
         byte var2 = -1;
         switch(var1.hashCode()) {
         case -2130199119:
            if(var1.equals("system_schema")) {
               var2 = 1;
            }
            break;
         case -887328209:
            if(var1.equals("system")) {
               var2 = 0;
            }
         }

         String var3;
         byte var4;
         switch(var2) {
         case 0:
            var3 = tableMetadata.name;
            var4 = -1;
            switch(var3.hashCode()) {
            case -380834451:
               if(var3.equals("built_views")) {
                  var4 = 5;
               }
               break;
            case -286998937:
               if(var3.equals("view_builds_in_progress")) {
                  var4 = 7;
               }
               break;
            case 103145323:
               if(var3.equals("local")) {
                  var4 = 0;
               }
               break;
            case 106543953:
               if(var3.equals("peers")) {
                  var4 = 1;
               }
               break;
            case 811841216:
               if(var3.equals("sstable_activity")) {
                  var4 = 2;
               }
               break;
            case 894963085:
               if(var3.equals("size_estimates")) {
                  var4 = 3;
               }
               break;
            case 1196124288:
               if(var3.equals("IndexInfo")) {
                  var4 = 4;
               }
               break;
            case 1813929228:
               if(var3.equals("available_ranges")) {
                  var4 = 6;
               }
            }

            switch(var4) {
            case 0:
            case 1:
               break;
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
               return new SystemKeyspacesFilteringRestrictions.SystemKeyspacesRestriction(tableMetadata, false);
            default:
               return new SystemKeyspacesFilteringRestrictions.DenyAllRestriction(tableMetadata.partitionKeyColumns());
            }
         default:
            return null;
         case 1:
            var3 = tableMetadata.name;
            var4 = -1;
            switch(var3.hashCode()) {
            case -1410341282:
               if(var3.equals("dropped_columns")) {
                  var4 = 2;
               }
               break;
            case -881377691:
               if(var3.equals("tables")) {
                  var4 = 0;
               }
               break;
            case -140572773:
               if(var3.equals("functions")) {
                  var4 = 4;
               }
               break;
            case 112204398:
               if(var3.equals("views")) {
                  var4 = 3;
               }
               break;
            case 949721053:
               if(var3.equals("columns")) {
                  var4 = 1;
               }
               break;
            case 1135524500:
               if(var3.equals("aggregates")) {
                  var4 = 5;
               }
            }

            switch(var4) {
            case 0:
            case 1:
            case 2:
            case 3:
               return new SystemKeyspacesFilteringRestrictions.SystemKeyspacesRestriction(tableMetadata, true);
            case 4:
            case 5:
            default:
               return new SystemKeyspacesFilteringRestrictions.SystemKeyspacesRestriction(tableMetadata, false);
            }
         }
      }
   }

   static {
      if(enabled()) {
         Resources.addReadableSystemResource(DataResource.table("system", "available_ranges"));
         Resources.addReadableSystemResource(DataResource.table("system", "size_estimates"));
         Resources.addReadableSystemResource(DataResource.table("system", "IndexInfo"));
         Resources.addReadableSystemResource(DataResource.table("system", "built_views"));
         Resources.addReadableSystemResource(DataResource.table("system", "sstable_activity"));
         Resources.addReadableSystemResource(DataResource.table("system", "view_builds_in_progress"));
      }

   }

   private static final class DenyAllRestriction implements AuthRestriction {
      private final ColumnMetadata column;

      DenyAllRestriction(List<ColumnMetadata> columns) {
         this.column = (ColumnMetadata)columns.get(0);
      }

      public void addRowFilterTo(RowFilter filter, QueryState state) {
         if(state.isOrdinaryUser()) {
            filter.addUserExpression(new SystemKeyspacesFilteringRestrictions.DenyAllRestriction.Expression(this.column));
         }

      }

      private static class Expression extends RowFilter.UserExpression {
         Expression(ColumnMetadata keyspaceColumn) {
            super(keyspaceColumn, Operator.EQ, ByteBufferUtil.EMPTY_BYTE_BUFFER);
         }

         protected void serialize(DataOutputPlus dataOutputPlus, ReadVerbs.ReadVersion version) throws IOException {
            throw new UnsupportedOperationException();
         }

         protected long serializedSize(ReadVerbs.ReadVersion version) {
            throw new UnsupportedOperationException();
         }

         public Flow<Boolean> isSatisfiedBy(TableMetadata cfMetaData, DecoratedKey decoratedKey, Row row) {
            return Flow.just(Boolean.valueOf(false));
         }
      }
   }

   private static final class SystemKeyspacesRestriction implements AuthRestriction {
      private final ColumnMetadata column;

      SystemKeyspacesRestriction(TableMetadata tableMetadata, boolean withTableInClustering) {
         this.column = withTableInClustering?(ColumnMetadata)tableMetadata.clusteringColumns().iterator().next():(ColumnMetadata)tableMetadata.partitionKeyColumns().iterator().next();
      }

      public void addRowFilterTo(RowFilter filter, QueryState state) {
         if(state.isOrdinaryUser()) {
            filter.addUserExpression(new SystemKeyspacesFilteringRestrictions.SystemKeyspacesRestriction.Expression(this.column, state));
         }

      }

      private static class Expression extends RowFilter.UserExpression {
         private final boolean withTableInClustering;
         private final QueryState state;

         Expression(ColumnMetadata column, QueryState state) {
            super(column, Operator.EQ, ByteBufferUtil.EMPTY_BYTE_BUFFER);
            this.state = state;
            this.withTableInClustering = column.isClusteringColumn();
         }

         protected void serialize(DataOutputPlus dataOutputPlus, ReadVerbs.ReadVersion version) throws IOException {
            throw new UnsupportedOperationException();
         }

         protected long serializedSize(ReadVerbs.ReadVersion version) {
            throw new UnsupportedOperationException();
         }

         public Flow<Boolean> isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row) {
            return Flow.just(Boolean.valueOf(this.isSatisfiedByInternal(metadata, partitionKey, row)));
         }

         private boolean isSatisfiedByInternal(TableMetadata tableMetadata, DecoratedKey decoratedKey, Row row) {
            ByteBuffer partitionKey = decoratedKey.getKey().duplicate();
            String keyspace = UTF8Serializer.instance.deserialize(partitionKey);
            if("system_schema".equals(tableMetadata.keyspace)) {
               byte var7 = -1;
               switch(keyspace.hashCode()) {
               case -2130199119:
                  if(keyspace.equals("system_schema")) {
                     var7 = 0;
                  }
                  break;
               case -887328209:
                  if(keyspace.equals("system")) {
                     var7 = 1;
                  }
               }

               switch(var7) {
               case 0:
                  return true;
               case 1:
                  if(row.isEmpty()) {
                     return true;
                  }

                  if(!this.withTableInClustering) {
                     return false;
                  }

                  String table = UTF8Serializer.instance.deserialize(row.clustering().clustering().getRawValues()[0]);
                  byte var10 = -1;
                  switch(table.hashCode()) {
                  case -380834451:
                     if(table.equals("built_views")) {
                        var10 = 5;
                     }
                     break;
                  case -286998937:
                     if(table.equals("view_builds_in_progress")) {
                        var10 = 7;
                     }
                     break;
                  case 103145323:
                     if(table.equals("local")) {
                        var10 = 0;
                     }
                     break;
                  case 106543953:
                     if(table.equals("peers")) {
                        var10 = 1;
                     }
                     break;
                  case 811841216:
                     if(table.equals("sstable_activity")) {
                        var10 = 2;
                     }
                     break;
                  case 894963085:
                     if(table.equals("size_estimates")) {
                        var10 = 3;
                     }
                     break;
                  case 1196124288:
                     if(table.equals("IndexInfo")) {
                        var10 = 4;
                     }
                     break;
                  case 1813929228:
                     if(table.equals("available_ranges")) {
                        var10 = 6;
                     }
                  }

                  switch(var10) {
                  case 0:
                  case 1:
                  case 2:
                  case 3:
                  case 4:
                  case 5:
                  case 6:
                  case 7:
                     return true;
                  default:
                     return false;
                  }
               }
            }

            return SystemKeyspacesFilteringRestrictions.checkDescribePermissionOnKeyspace(keyspace, this.state.getUserRolesAndPermissions());
         }
      }
   }
}
