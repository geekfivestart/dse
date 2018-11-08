package org.apache.cassandra.cql3.statements;

import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.transport.messages.ResultMessage;

public class ListUsersStatement extends ListRolesStatement {
   private static final String KS = "system_auth";
   private static final String CF = "users";
   private static final List<ColumnSpecification> metadata;

   public ListUsersStatement() {
   }

   protected ResultMessage formatResults(List<RoleResource> sortedRoles) {
      ResultSet.ResultMetadata resultMetadata = new ResultSet.ResultMetadata(metadata);
      ResultSet result = new ResultSet(resultMetadata);
      IRoleManager roleManager = DatabaseDescriptor.getRoleManager();
      Iterator var5 = sortedRoles.iterator();

      while(var5.hasNext()) {
         RoleResource role = (RoleResource)var5.next();
         if(roleManager.canLogin(role)) {
            result.addColumnValue(UTF8Type.instance.decompose(role.getRoleName()));
            result.addColumnValue(BooleanType.instance.decompose(Boolean.valueOf(roleManager.hasSuperuserStatus(role))));
         }
      }

      return new ResultMessage.Rows(result);
   }

   static {
      metadata = ImmutableList.of(new ColumnSpecification("system_auth", "users", new ColumnIdentifier("name", true), UTF8Type.instance), new ColumnSpecification("system_auth", "users", new ColumnIdentifier("super", true), BooleanType.instance));
   }
}
