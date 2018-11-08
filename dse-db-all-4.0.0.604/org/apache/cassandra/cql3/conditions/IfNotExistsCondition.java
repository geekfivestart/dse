package org.apache.cassandra.cql3.conditions;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.CQL3CasRequest;
import org.apache.cassandra.db.Clustering;

final class IfNotExistsCondition extends AbstractConditions {
   IfNotExistsCondition() {
   }

   public void addConditionsTo(CQL3CasRequest request, Clustering clustering, QueryOptions options) {
      request.addNotExist(clustering);
   }

   public boolean isIfNotExists() {
      return true;
   }
}
