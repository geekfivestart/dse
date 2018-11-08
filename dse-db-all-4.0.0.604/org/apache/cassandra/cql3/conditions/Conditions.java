package org.apache.cassandra.cql3.conditions;

import java.util.List;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.CQL3CasRequest;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.schema.ColumnMetadata;

public interface Conditions {
   Conditions EMPTY_CONDITION = ColumnConditions.newBuilder().build();
   Conditions IF_EXISTS_CONDITION = new IfExistsCondition();
   Conditions IF_NOT_EXISTS_CONDITION = new IfNotExistsCondition();

   void addFunctionsTo(List<Function> var1);

   Iterable<ColumnMetadata> getColumns();

   boolean isEmpty();

   boolean isIfExists();

   boolean isIfNotExists();

   boolean appliesToStaticColumns();

   boolean appliesToRegularColumns();

   void addConditionsTo(CQL3CasRequest var1, Clustering var2, QueryOptions var3);
}
