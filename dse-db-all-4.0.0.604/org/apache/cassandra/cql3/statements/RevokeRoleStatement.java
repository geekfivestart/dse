package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import io.reactivex.Single;
import java.util.concurrent.Callable;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class RevokeRoleStatement extends RoleManagementStatement {
   public RevokeRoleStatement(RoleName name, RoleName grantee) {
      super(name, grantee);
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.REVOKE;
   }

   public Single<ResultMessage> execute(QueryState state) {
      return Single.fromCallable(() -> {
         DatabaseDescriptor.getRoleManager().revokeRole(state.getUser(), this.role, this.grantee);
         return new ResultMessage.Void();
      });
   }
}
