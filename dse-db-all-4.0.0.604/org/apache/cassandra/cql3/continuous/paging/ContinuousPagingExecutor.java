package org.apache.cassandra.cql3.continuous.paging;

import java.util.concurrent.TimeUnit;
import org.apache.cassandra.cql3.selection.ResultBuilder;
import org.apache.cassandra.service.pager.PagingState;

public interface ContinuousPagingExecutor {
   long localStartTimeInMillis();

   long queryStartTimeInNanos();

   boolean isLocalQuery();

   PagingState state(boolean var1);

   default void schedule(Runnable runnable) {
      this.schedule(runnable, 0L, TimeUnit.NANOSECONDS);
   }

   void schedule(Runnable var1, long var2, TimeUnit var4);

   void schedule(PagingState var1, ResultBuilder var2);

   int coreId();

   void setCoreId(int var1);
}
