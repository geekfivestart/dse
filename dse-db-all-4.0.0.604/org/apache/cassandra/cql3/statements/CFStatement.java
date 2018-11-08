package org.apache.cassandra.cql3.statements;

import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientState;

public abstract class CFStatement extends ParsedStatement implements KeyspaceStatement {
   protected final CFName cfName;

   protected CFStatement(CFName cfName) {
      this.cfName = cfName;
   }

   public void prepareKeyspace(ClientState state) throws InvalidRequestException {
      if(!this.cfName.hasKeyspace()) {
         this.cfName.setKeyspace(state.getKeyspace(), true);
      }

   }

   public void prepareKeyspace(String keyspace) {
      if(!this.cfName.hasKeyspace()) {
         this.cfName.setKeyspace(keyspace, true);
      }

   }

   public String keyspace() {
      assert this.cfName.hasKeyspace() : "The statement hasn't be prepared correctly";

      return this.cfName.getKeyspace();
   }

   public String columnFamily() {
      return this.cfName.getColumnFamily();
   }
}
