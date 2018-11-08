package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.reactivex.Single;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.auth.Role;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class ListRolesStatement extends AuthorizationStatement {
   private static final String KS = "system_auth";
   private static final String CF = "roles";
   private static final MapType optionsType;
   private static final List<ColumnSpecification> metadata;
   private final RoleResource grantee;
   private final boolean recursive;

   public ListRolesStatement() {
      this(new RoleName(), false);
   }

   public ListRolesStatement(RoleName grantee, boolean recursive) {
      this.grantee = grantee.hasName()?RoleResource.role(grantee.getName()):null;
      this.recursive = recursive;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.LIST_ROLES;
   }

   public void validate(QueryState state) {
   }

   public void checkAccess(QueryState state) {
      state.checkNotAnonymous();
      if(this.grantee != null && !DatabaseDescriptor.getRoleManager().isExistingRole(this.grantee)) {
         throw RequestValidations.invalidRequest("%s doesn't exist", new Object[]{this.grantee});
      }
   }

   public Single<ResultMessage> execute(QueryState state) throws RequestValidationException, RequestExecutionException {
      return Single.defer(() -> {
         boolean hasRootLevelSelect = state.hasRolePermission(RoleResource.root(), CorePermission.DESCRIBE);
         if(hasRootLevelSelect) {
            return this.grantee == null?this.resultMessage(DatabaseDescriptor.getRoleManager().getAllRoles()):this.resultMessage(DatabaseDescriptor.getRoleManager().getRoles(this.grantee, this.recursive));
         } else {
            RoleResource currentUser = RoleResource.role(state.getUser().getName());
            return this.grantee == null?this.resultMessage(DatabaseDescriptor.getRoleManager().getRoles(currentUser, this.recursive)):(DatabaseDescriptor.getRoleManager().getRoles(currentUser, true).contains(this.grantee)?this.resultMessage(DatabaseDescriptor.getRoleManager().getRoles(this.grantee, this.recursive)):Single.error(new UnauthorizedException(String.format("You are not authorized to view roles granted to %s ", new Object[]{this.grantee.getRoleName()}))));
         }
      });
   }

   private Single<ResultMessage> resultMessage(Set<RoleResource> roles) {
      if(roles.isEmpty()) {
         return Single.just(new ResultMessage.Void());
      } else {
         List<RoleResource> sorted = Lists.newArrayList(roles);
         Collections.sort(sorted);
         return Single.just(this.formatResults(sorted));
      }
   }

   protected ResultMessage formatResults(List<RoleResource> sortedRoles) {
      ResultSet.ResultMetadata resultMetadata = new ResultSet.ResultMetadata(metadata);
      ResultSet result = new ResultSet(resultMetadata);
      IRoleManager roleManager = DatabaseDescriptor.getRoleManager();
      Iterator var5 = sortedRoles.iterator();

      while(var5.hasNext()) {
         RoleResource role = (RoleResource)var5.next();
         Role data = roleManager.getRoleData(role);
         result.addColumnValue(UTF8Type.instance.decompose(role.getRoleName()));
         result.addColumnValue(BooleanType.instance.decompose(Boolean.valueOf(data.isSuper)));
         result.addColumnValue(BooleanType.instance.decompose(Boolean.valueOf(data.canLogin)));
         result.addColumnValue(optionsType.decompose(data.options));
      }

      return new ResultMessage.Rows(result);
   }

   static {
      optionsType = MapType.getInstance(UTF8Type.instance, UTF8Type.instance, false);
      metadata = ImmutableList.of(new ColumnSpecification("system_auth", "roles", new ColumnIdentifier("role", true), UTF8Type.instance), new ColumnSpecification("system_auth", "roles", new ColumnIdentifier("super", true), BooleanType.instance), new ColumnSpecification("system_auth", "roles", new ColumnIdentifier("login", true), BooleanType.instance), new ColumnSpecification("system_auth", "roles", new ColumnIdentifier("options", true), optionsType));
   }
}
