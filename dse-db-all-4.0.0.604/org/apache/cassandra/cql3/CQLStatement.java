package org.apache.cassandra.cql3;

import com.datastax.bdp.db.audit.AuditableEventType;
import io.reactivex.Single;
import javax.annotation.Nullable;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public interface CQLStatement {
   AuditableEventType getAuditEventType();

   int getBoundTerms();

   void checkAccess(QueryState var1);

   void validate(QueryState var1) throws RequestValidationException;

   Single<? extends ResultMessage> execute(QueryState var1, QueryOptions var2, long var3) throws RequestValidationException, RequestExecutionException;

   Single<? extends ResultMessage> executeInternal(QueryState var1, QueryOptions var2) throws RequestValidationException, RequestExecutionException;

   Iterable<Function> getFunctions();

   @Nullable
   StagedScheduler getScheduler();

   default boolean hasConditions() {
      return false;
   }
}
