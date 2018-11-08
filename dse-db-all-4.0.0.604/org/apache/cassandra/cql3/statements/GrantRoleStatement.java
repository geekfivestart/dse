package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import io.reactivex.Single;
import java.util.concurrent.Callable;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class GrantRoleStatement extends RoleManagementStatement {
   public GrantRoleStatement(RoleName name, RoleName grantee) {
      super(name, grantee);
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.GRANT;
   }

   public Single<ResultMessage> execute(QueryState state) {
      return Single.fromCallable(() -> {
         DatabaseDescriptor.getRoleManager().grantRole(state.getUser(), this.role, this.grantee);
         return new ResultMessage.Void();
      });
   }
}
