package org.apache.cassandra.auth;

import java.util.List;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.SchemaChangeListener;

public class AuthSchemaChangeListener extends SchemaChangeListener {
   public AuthSchemaChangeListener() {
   }

   public void onDropKeyspace(String ksName) {
      DatabaseDescriptor.getAuthorizer().revokeAllOn(DataResource.keyspace(ksName));
      DatabaseDescriptor.getAuthorizer().revokeAllOn(FunctionResource.keyspace(ksName));
   }

   public void onDropTable(String ksName, String cfName) {
      DatabaseDescriptor.getAuthorizer().revokeAllOn(DataResource.table(ksName, cfName));
   }

   public void onDropFunction(String ksName, String functionName, List<AbstractType<?>> argTypes) {
      DatabaseDescriptor.getAuthorizer().revokeAllOn(FunctionResource.function(ksName, functionName, argTypes));
   }

   public void onDropAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes) {
      DatabaseDescriptor.getAuthorizer().revokeAllOn(FunctionResource.function(ksName, aggregateName, argTypes));
   }
}
