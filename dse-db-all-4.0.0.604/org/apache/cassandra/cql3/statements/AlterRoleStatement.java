package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import io.reactivex.Single;
import java.util.Iterator;
import java.util.concurrent.Callable;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.auth.RoleOptions;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class AlterRoleStatement extends AuthenticationStatement {
   private final RoleResource role;
   private final RoleOptions opts;

   public AlterRoleStatement(RoleName name, RoleOptions opts) {
      this.role = RoleResource.role(name.getName());
      this.opts = opts;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.ALTER_ROLE;
   }

   public void validate(QueryState state) {
      this.opts.validate();
      if(this.opts.isEmpty()) {
         throw new InvalidRequestException("ALTER [ROLE|USER] can't be empty");
      }
   }

   public void checkAccess(QueryState state) {
      IRoleManager roleManager = DatabaseDescriptor.getRoleManager();
      state.checkNotAnonymous();
      if(!roleManager.isExistingRole(this.role)) {
         throw new InvalidRequestException(String.format("%s doesn't exist", new Object[]{this.role.getRoleName()}));
      } else if(this.opts.getSuperuser().isPresent() && state.hasRole(this.role)) {
         throw new UnauthorizedException("You aren't allowed to alter your own superuser status or that of a role granted to you");
      } else {
         boolean isSuper = state.isSuper();
         if(this.opts.getSuperuser().isPresent() && !isSuper) {
            throw new UnauthorizedException("Only superusers are allowed to alter superuser status");
         } else if(!isSuper) {
            if(state.getUserName().equals(this.role.getRoleName())) {
               Iterator var4 = this.opts.getOptions().keySet().iterator();

               while(var4.hasNext()) {
                  IRoleManager.Option option = (IRoleManager.Option)var4.next();
                  if(!roleManager.alterableOptions().contains(option)) {
                     throw new UnauthorizedException(String.format("You aren't allowed to alter %s", new Object[]{option}));
                  }
               }
            } else {
               super.checkPermission(state, CorePermission.ALTER, this.role);
            }

         }
      }
   }

   public Single<ResultMessage> execute(QueryState state) throws RequestValidationException, RequestExecutionException {
      return Single.fromCallable(() -> {
         if(!this.opts.isEmpty()) {
            DatabaseDescriptor.getRoleManager().alterRole(state.getUser(), this.role, this.opts);
         }

         return new ResultMessage.Void();
      });
   }
}
