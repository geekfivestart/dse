package org.apache.cassandra.cql3.statements;

import java.util.Collections;
import java.util.List;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.utils.MD5Digest;

public abstract class ParsedStatement {
   private VariableSpecifications variables;

   public ParsedStatement() {
   }

   public VariableSpecifications getBoundVariables() {
      return this.variables;
   }

   public void setBoundVariables(List<ColumnIdentifier> boundNames) {
      this.variables = new VariableSpecifications(boundNames);
   }

   public void setBoundVariables(VariableSpecifications variables) {
      this.variables = variables;
   }

   public abstract ParsedStatement.Prepared prepare() throws RequestValidationException;

   public Iterable<Function> getFunctions() {
      return Collections.emptyList();
   }

   public static class Prepared {
      public String rawCQLStatement;
      public final MD5Digest resultMetadataId;
      public final List<ColumnSpecification> boundNames;
      public final CQLStatement statement;
      public final short[] partitionKeyBindIndexes;

      protected Prepared(CQLStatement statement, List<ColumnSpecification> boundNames, short[] partitionKeyBindIndexes) {
         this.statement = statement;
         this.boundNames = boundNames;
         this.partitionKeyBindIndexes = partitionKeyBindIndexes;
         this.resultMetadataId = ResultSet.ResultMetadata.fromPrepared(this).getResultMetadataId();
         this.rawCQLStatement = "";
      }

      public Prepared(CQLStatement statement, VariableSpecifications names, short[] partitionKeyBindIndexes) {
         this(statement, names.getSpecifications(), partitionKeyBindIndexes);
      }

      public Prepared(CQLStatement statement) {
         this(statement, (List)Collections.emptyList(), (short[])null);
      }
   }
}
