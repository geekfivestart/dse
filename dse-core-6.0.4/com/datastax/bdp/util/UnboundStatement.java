package com.datastax.bdp.util;

import java.nio.ByteBuffer;
import java.util.List;

public class UnboundStatement {
   public final String cql;
   public final List<ByteBuffer> variables;

   public UnboundStatement(String cql, List<ByteBuffer> variables) {
      this.cql = cql;
      this.variables = variables;
   }

   public String toString() {
      return "UnboundStatement{cql=" + this.cql + ", variables=" + this.variables + '}';
   }
}
