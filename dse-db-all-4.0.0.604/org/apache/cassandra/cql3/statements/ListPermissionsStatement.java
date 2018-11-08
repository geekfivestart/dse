package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import io.reactivex.Single;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.cassandra.auth.GrantMode;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.PermissionDetails;
import org.apache.cassandra.auth.Resources;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class ListPermissionsStatement extends AuthorizationStatement {
   private static final String KS = "system_auth";
   private static final String CF = "permissions";
   private static final List<ColumnSpecification> metadata;
   protected final Set<Permission> permissions;
   protected IResource resource;
   protected final boolean recursive;
   private final RoleResource grantee;

   public ListPermissionsStatement(Set<Permission> permissions, IResource resource, RoleName grantee, boolean recursive) {
      this.permissions = permissions;
      this.resource = resource;
      this.recursive = recursive;
      this.grantee = grantee.hasName()?RoleResource.role(grantee.getName()):null;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.LIST_PERMISSIONS;
   }

   public void validate(QueryState state) {
      if(!DatabaseDescriptor.getAuthorizer().requireAuthorization()) {
         throw RequestValidations.invalidRequest(String.format("LIST PERMISSIONS operation is not supported by the %s if it is not enabled", new Object[]{DatabaseDescriptor.getAuthorizer().implementation().getClass().getSimpleName()}));
      }
   }

   public void checkAccess(QueryState state) {
      state.checkNotAnonymous();
      if(this.resource != null) {
         this.resource = maybeCorrectResource(this.resource, state.getClientState());
         if(!this.resource.exists()) {
            throw RequestValidations.invalidRequest("%s doesn't exist", new Object[]{this.resource});
         }
      }

      if(this.grantee != null && !DatabaseDescriptor.getRoleManager().isExistingRole(this.grantee)) {
         throw RequestValidations.invalidRequest("%s doesn't exist", new Object[]{this.grantee});
      } else if(!state.isSuper() && !state.isSystem() && !state.hasRole(this.grantee) && !state.hasRolePermission(this.grantee != null?this.grantee:RoleResource.root(), CorePermission.DESCRIBE)) {
         throw new UnauthorizedException(String.format("You are not authorized to view %s's permissions", new Object[]{this.grantee == null?"everyone":this.grantee.getRoleName()}));
      }
   }

   public Single<ResultMessage> execute(QueryState state) throws RequestValidationException, RequestExecutionException {
      return Single.fromCallable(() -> {
         List<PermissionDetails> details = new ArrayList();
         if(this.resource != null && this.recursive) {
            Iterator var2 = Resources.chain(this.resource).iterator();

            while(var2.hasNext()) {
               IResource r = (IResource)var2.next();
               details.addAll(this.list(r));
            }
         } else {
            details.addAll(this.list(this.resource));
         }

         Collections.sort(details);
         return this.resultMessage(details);
      });
   }

   private Set<PermissionDetails> list(IResource resource) throws RequestValidationException, RequestExecutionException {
      try {
         return DatabaseDescriptor.getAuthorizer().list(this.permissions, resource, this.grantee);
      } catch (UnsupportedOperationException var3) {
         throw new InvalidRequestException(var3.getMessage());
      }
   }

   private ResultMessage resultMessage(List<PermissionDetails> details) {
      if(details.isEmpty()) {
         return new ResultMessage.Void();
      } else {
         ResultSet.ResultMetadata resultMetadata = new ResultSet.ResultMetadata(metadata);
         ResultSet result = new ResultSet(resultMetadata);
         Iterator var4 = details.iterator();

         while(var4.hasNext()) {
            PermissionDetails pd = (PermissionDetails)var4.next();
            result.addColumnValue(UTF8Type.instance.decompose(pd.grantee));
            result.addColumnValue(UTF8Type.instance.decompose(pd.grantee));
            result.addColumnValue(UTF8Type.instance.decompose(pd.resource.toString()));
            result.addColumnValue(UTF8Type.instance.decompose(pd.permission.getFullName()));
            result.addColumnValue(BooleanType.instance.decompose(Boolean.valueOf(pd.modes.contains(GrantMode.GRANT))));
            result.addColumnValue(BooleanType.instance.decompose(Boolean.valueOf(pd.modes.contains(GrantMode.RESTRICT))));
            result.addColumnValue(BooleanType.instance.decompose(Boolean.valueOf(pd.modes.contains(GrantMode.GRANTABLE))));
         }

         return new ResultMessage.Rows(result);
      }
   }

   static {
      metadata = Collections.unmodifiableList(Arrays.asList(new ColumnSpecification[]{new ColumnSpecification("system_auth", "permissions", new ColumnIdentifier("role", true), UTF8Type.instance), new ColumnSpecification("system_auth", "permissions", new ColumnIdentifier("username", true), UTF8Type.instance), new ColumnSpecification("system_auth", "permissions", new ColumnIdentifier("resource", true), UTF8Type.instance), new ColumnSpecification("system_auth", "permissions", new ColumnIdentifier("permission", true), UTF8Type.instance), new ColumnSpecification("system_auth", "permissions", new ColumnIdentifier("granted", true), BooleanType.instance), new ColumnSpecification("system_auth", "permissions", new ColumnIdentifier("restricted", true), BooleanType.instance), new ColumnSpecification("system_auth", "permissions", new ColumnIdentifier("grantable", true), BooleanType.instance)}));
   }
}
