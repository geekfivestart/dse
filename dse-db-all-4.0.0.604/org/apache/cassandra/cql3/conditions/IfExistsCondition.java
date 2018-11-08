package org.apache.cassandra.cql3.conditions;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.CQL3CasRequest;
import org.apache.cassandra.db.Clustering;

final class IfExistsCondition extends AbstractConditions {
   IfExistsCondition() {
   }

   public void addConditionsTo(CQL3CasRequest request, Clustering clustering, QueryOptions options) {
      request.addExist(clustering);
   }

   public boolean isIfExists() {
      return true;
   }
}
