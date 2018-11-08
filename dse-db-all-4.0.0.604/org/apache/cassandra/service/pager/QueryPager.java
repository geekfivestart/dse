package org.apache.cassandra.service.pager;

import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.ReadContext;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.utils.flow.Flow;

public interface QueryPager {
   QueryPager EMPTY = new QueryPager() {
      public Flow<FlowablePartition> fetchPage(PageSize pageSize, ReadContext ctx) throws RequestValidationException, RequestExecutionException {
         return Flow.empty();
      }

      public Flow<FlowablePartition> fetchPageInternal(PageSize pageSize) throws RequestValidationException, RequestExecutionException {
         return Flow.empty();
      }

      public boolean isExhausted() {
         return true;
      }

      public int maxRemaining() {
         return 0;
      }

      public PagingState state(boolean inclusive) {
         return null;
      }

      public QueryPager withUpdatedLimit(DataLimits newLimits) {
         throw new UnsupportedOperationException();
      }
   };

   Flow<FlowablePartition> fetchPage(PageSize var1, ReadContext var2) throws RequestValidationException, RequestExecutionException;

   Flow<FlowablePartition> fetchPageInternal(PageSize var1) throws RequestValidationException, RequestExecutionException;

   boolean isExhausted();

   int maxRemaining();

   PagingState state(boolean var1);

   QueryPager withUpdatedLimit(DataLimits var1);
}
