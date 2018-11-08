package com.datastax.bdp.cassandra.auth;

import com.datastax.bdp.config.DseConfig;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.CassandraAuthorizer;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.GrantMode;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.PermissionSets;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.IAuthorizer.TransitionalMode;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.auth.permission.Permissions;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.auth.user.UserRolesAndPermissions.RoleResourcePermissionFilter;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.UseStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage.Rows;
import org.apache.cassandra.utils.ByteBufferUtil;

public class DseAuthorizer extends CassandraAuthorizer {
   private static final Set<Permission> extendedRolePermissions;
   protected boolean enabled;
   protected boolean rowLevelEnabled;
   protected TransitionalMode transitionalMode;
   private SelectStatement activeResourcesStatement;

   public DseAuthorizer() {
      this.transitionalMode = TransitionalMode.DISABLED;
   }

   public void setup() {
      super.setup();
      String query = String.format("SELECT role, resource FROM %s.%s", new Object[]{"system_auth", "role_permissions"});
      this.activeResourcesStatement = (SelectStatement)QueryProcessor.getStatement(query, QueryState.forInternalCalls()).statement;
   }

   public Single<QueryState> getQueryState(QueryState loginState, Map<String, ByteBuffer> customPayload, CQLStatement statement) throws CharacterCodingException {
      ClientState clientState = loginState.getClientState();
      AuthenticatedUser user = clientState.getUser();
      String proxiedUserName = ByteBufferUtil.string((ByteBuffer)customPayload.get("ProxyExecute"));
      if(!loginState.hasPermission(RoleResource.role(proxiedUserName), ProxyPermission.EXECUTE)) {
         throw new UnauthorizedException(String.format("Either '%s' does not have permission to execute queries as '%s' or that role does not exist. Run 'GRANT PROXY.EXECUTE ON ROLE '%s' TO '%s' as an administrator if you wish to allow this.", new Object[]{user.getName(), proxiedUserName, proxiedUserName, user.getName()}));
      } else {
         AuthenticatedUser authenticatedUser = DseAuthenticator.proxy(user, proxiedUserName);
         if(statement instanceof UseStatement) {
            throw new InvalidRequestException("USE statements cannot be executed as another user.  To use DSE proxy execution most efficiently, prepare your statements once beforehand (as your normal login user) and then proxy execute them multiple times.  If you really need USE, you can execute it as your normal login user and the selected keyspace will be used for proxy executed queries.");
         } else {
            ClientState proxyClientState = ClientState.forExternalCalls(clientState.getRemoteAddress(), clientState.connection);
            if(clientState.getRawKeyspace() != null) {
               proxyClientState.setKeyspace(clientState.getKeyspace());
            }

            return proxyClientState.login(authenticatedUser).flatMap((ignore) -> {
               return DatabaseDescriptor.getAuthManager().getUserRolesAndPermissions(authenticatedUser);
            }).map((u) -> {
               if(u.isSuper()) {
                  throw new UnauthorizedException("Cannot proxy as a super user.");
               } else {
                  return new QueryState(proxyClientState, loginState.getStreamId(), u);
               }
            });
         }
      }
   }

   public Map<IResource, PermissionSets> allPermissionSets(RoleResource role) {
      if(!this.enabled) {
         return new DseAuthorizer.PermissionsMap(DseAuthorizer::applicablePermissionsTransform, null);
      } else {
         switch(null.$SwitchMap$org$apache$cassandra$auth$IAuthorizer$TransitionalMode[this.transitionalMode.ordinal()]) {
         case 1:
            return super.allPermissionSets(role);
         case 2:
            return this.allTransitionalPermissionSets(role);
         case 3:
            if(role.equals(AuthenticatedUser.ANONYMOUS_USER.getPrimaryRole())) {
               return this.allTransitionalPermissionSets(role);
            }

            return super.allPermissionSets(role);
         default:
            throw new AssertionError("Unknown transitionalMode " + this.transitionalMode);
         }
      }
   }

   private DseAuthorizer.PermissionsMap allTransitionalPermissionSets(RoleResource role) {
      return new DseAuthorizer.PermissionsMap(((Boolean)DatabaseDescriptor.getAuthManager().hasSuperUserStatus(role).blockingGet()).booleanValue()?DseAuthorizer::allPermissionsTransform:DseAuthorizer::transitionalPermissionsTransform, null);
   }

   private static PermissionSets allPermissionsTransform(IResource resource) {
      return PermissionSets.builder().addGranted(Permissions.all()).build();
   }

   private static PermissionSets transitionalPermissionsTransform(IResource resource) {
      return PermissionSets.builder().addGranted(Permissions.all()).removeGranted(CorePermission.AUTHORIZE).removeGranted(CorePermission.READ).removeGranted(CorePermission.WRITE).build();
   }

   private static PermissionSets applicablePermissionsTransform(IResource resource) {
      return PermissionSets.builder().addGranted(applicablePermissionsInternal(resource)).build();
   }

   private static Set<Permission> applicablePermissionsInternal(IResource resource) {
      return resource instanceof RoleResource && resource.hasParent()?extendedRolePermissions:resource.applicablePermissions();
   }

   public void revokeAllFrom(RoleResource droppedRole) {
      if(this.enabled) {
         super.revokeAllFrom(droppedRole);
      }

   }

   public Set<RoleResource> revokeAllOn(IResource droppedResource) {
      if(!this.enabled) {
         return Collections.emptySet();
      } else {
         Set<RoleResource> roleResources = new HashSet(super.revokeAllOn(droppedResource));
         DseResourceFactory dseResourceFactory = new DseResourceFactory();
         dseResourceFactory.getExtensions(droppedResource).forEach((resource) -> {
            roleResources.addAll(super.revokeAllOn(resource));
         });
         if(droppedResource instanceof DataResource) {
            DataResource dataResource = (DataResource)droppedResource;
            QueryOptions options = QueryOptions.forInternalCalls(ConsistencyLevel.LOCAL_ONE, Collections.emptyList());
            Rows rows = (Rows)TPCUtils.blockingGet(this.activeResourcesStatement.execute(QueryState.forInternalCalls(), options, System.nanoTime()));
            UntypedResultSet result = UntypedResultSet.create(rows.result);
            Iterator var8 = result.iterator();

            while(var8.hasNext()) {
               Row row = (Row)var8.next();
               String resource = row.getString("resource");
               if(resource.startsWith("rows") && resource.endsWith(dataResource.getName())) {
                  IResource dseRowResource = dseResourceFactory.fromName(resource);
                  this.revoke(AuthenticatedUser.SYSTEM_USER, dseRowResource.applicablePermissions(), dseRowResource, RoleResource.role(row.getString("role")), new GrantMode[]{GrantMode.GRANT, GrantMode.GRANTABLE, GrantMode.RESTRICT});
               }
            }
         }

         return roleResources;
      }
   }

   Set<String> findRowTargetsForUser(QueryState state, DataResource dataResource, Permission permission) {
      UserRolesAndPermissions userRolesAndPermissions = state.getUserRolesAndPermissions();
      return (Set)userRolesAndPermissions.filterPermissions((s) -> {
         return s;
      }, HashSet::<init>, (s, role, resource, permissionSets) -> {
         if(resource instanceof DseRowResource) {
            DseRowResource dseRowResource = (DseRowResource)resource;
            if(dseRowResource.getParent().equals(dataResource) && permissionSets.hasEffectivePermission(permission)) {
               s.add(dseRowResource.getRowTarget());
            }
         }

         return s;
      });
   }

   public boolean isRowLevelEnabled() {
      return this.rowLevelEnabled;
   }

   public boolean requireAuthorization() {
      return this.enabled;
   }

   public TransitionalMode getTransitionalMode() {
      return this.transitionalMode;
   }

   public void validateConfiguration() throws ConfigurationException {
      this.enabled = DseConfig.isAuthorizationEnabled();
      this.rowLevelEnabled = DseConfig.isRowLevelAuthorizationEnabled();
      this.transitionalMode = TransitionalMode.valueOf(DseConfig.getAuthorizationTransitionalMode().toUpperCase());
   }

   public Set<Permission> applicablePermissions(IResource resource) {
      return applicablePermissionsInternal(resource);
   }

   static {
      extendedRolePermissions = Permissions.immutableSetOf(new Permission[]{CorePermission.ALTER, CorePermission.DROP, CorePermission.AUTHORIZE, ProxyPermission.EXECUTE, ProxyPermission.LOGIN});
   }

   private final class PermissionsMap extends AbstractMap<IResource, PermissionSets> {
      private final java.util.function.Function<IResource, PermissionSets> function;

      private PermissionsMap(java.util.function.Function<IResource, PermissionSets> var1) {
         this.function = function;
      }

      public Set<Entry<IResource, PermissionSets>> entrySet() {
         throw new UnsupportedOperationException();
      }

      public PermissionSets get(Object key) {
         return (PermissionSets)this.function.apply((IResource)key);
      }
   }
}
