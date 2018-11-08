package org.apache.cassandra.service.pager;

import io.reactivex.functions.Action;
import java.util.Arrays;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.ReadContext;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.flow.Flow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiPartitionPager implements QueryPager {
   private static final Logger logger = LoggerFactory.getLogger(MultiPartitionPager.class);
   private final SinglePartitionPager[] pagers;
   private final DataLimits limits;
   private final int nowInSec;
   private int remaining;
   private int current;

   public MultiPartitionPager(SinglePartitionReadCommand.Group group, PagingState state, ProtocolVersion protocolVersion) {
      this.limits = group.limits();
      this.nowInSec = group.nowInSec();
      int i = 0;
      if(state != null) {
         while(i < group.commands.size() && !((SinglePartitionReadCommand)group.commands.get(i)).partitionKey().getKey().equals(state.partitionKey)) {
            ++i;
         }
      }

      if(i >= group.commands.size()) {
         this.pagers = null;
      } else {
         this.pagers = new SinglePartitionPager[group.commands.size() - i];
         SinglePartitionReadCommand command = (SinglePartitionReadCommand)group.commands.get(i);
         this.pagers[0] = command.getPager(state, protocolVersion);

         for(int j = i + 1; j < group.commands.size(); ++j) {
            this.pagers[j - i] = ((SinglePartitionReadCommand)group.commands.get(j)).getPager((PagingState)null, protocolVersion);
         }

         this.remaining = state == null?this.limits.count():state.remaining;
      }
   }

   private MultiPartitionPager(SinglePartitionPager[] pagers, DataLimits limits, int nowInSec, int remaining, int current) {
      this.pagers = pagers;
      this.limits = limits;
      this.nowInSec = nowInSec;
      this.remaining = remaining;
      this.current = current;
   }

   public QueryPager withUpdatedLimit(DataLimits newLimits) {
      SinglePartitionPager[] newPagers = (SinglePartitionPager[])Arrays.copyOf(this.pagers, this.pagers.length);
      newPagers[this.current] = newPagers[this.current].withUpdatedLimit(newLimits);
      return new MultiPartitionPager(newPagers, newLimits, this.nowInSec, this.remaining, this.current);
   }

   public PagingState state(boolean inclusive) {
      if(this.isExhausted()) {
         return null;
      } else {
         PagingState state = this.pagers[this.current].state(inclusive);
         if(logger.isTraceEnabled()) {
            logger.trace("{} - state: {}, current: {}", new Object[]{Integer.valueOf(this.hashCode()), state, Integer.valueOf(this.current)});
         }

         int remaining = inclusive?FBUtilities.add(this.remaining, 1):this.remaining;
         int remainingInPartition = inclusive?FBUtilities.add(this.pagers[this.current].remainingInPartition(), 1):this.pagers[this.current].remainingInPartition();
         return new PagingState(this.pagers[this.current].key(), state == null?null:state.rowMark, remaining, remainingInPartition, inclusive);
      }
   }

   public boolean isExhausted() {
      if(this.remaining > 0 && this.pagers != null) {
         while(this.current < this.pagers.length) {
            if(!this.pagers[this.current].isExhausted()) {
               return false;
            }

            if(logger.isTraceEnabled()) {
               logger.trace("{}, current: {} -> {}", new Object[]{Integer.valueOf(this.hashCode()), Integer.valueOf(this.current), Integer.valueOf(this.current + 1)});
            }

            ++this.current;
            if(this.current < this.pagers.length) {
               DataLimits limits = this.pagers[this.current].limits().withCount(this.remaining);
               this.pagers[this.current] = this.pagers[this.current].withUpdatedLimit(limits);
            }
         }

         return true;
      } else {
         return true;
      }
   }

   public Flow<FlowablePartition> fetchPage(PageSize pageSize, ReadContext ctx) throws RequestValidationException, RequestExecutionException {
      return (new MultiPartitionPager.MultiPartitions(pageSize, ctx)).partitions();
   }

   public Flow<FlowablePartition> fetchPageInternal(PageSize pageSize) throws RequestValidationException, RequestExecutionException {
      return (new MultiPartitionPager.MultiPartitions(pageSize, (ReadContext)null)).partitions();
   }

   public int maxRemaining() {
      return this.remaining;
   }

   private class MultiPartitions {
      private final PageSize pageSize;
      private PageSize toQuery;
      @Nullable
      private final ReadContext ctx;
      private int counted;

      private MultiPartitions(PageSize pageSize, ReadContext ctx) {
         this.pageSize = pageSize;
         this.ctx = ctx;
      }

      private Flow<FlowablePartition> partitions() {
         return this.fetchSubPage((DataLimits.Counter)null).concatWith(this::moreContents).doOnClose(this::close);
      }

      protected Flow<FlowablePartition> moreContents() {
         DataLimits.Counter currentCounter = MultiPartitionPager.this.pagers[MultiPartitionPager.this.current].lastCounter;
         this.counted += currentCounter.counted();
         MultiPartitionPager.this.remaining = MultiPartitionPager.this.remaining - currentCounter.counted();
         boolean isDone = this.toQuery.isComplete(currentCounter.counted(), currentCounter.bytesCounted()) || MultiPartitionPager.this.limits.isGroupByLimit() && !MultiPartitionPager.this.pagers[MultiPartitionPager.this.current].isExhausted();
         if(MultiPartitionPager.logger.isTraceEnabled()) {
            MultiPartitionPager.logger.trace("{} - moreContents, current: {}, counted: {}, isDone: {}", new Object[]{Integer.valueOf(MultiPartitionPager.this.hashCode()), Integer.valueOf(MultiPartitionPager.this.current), Integer.valueOf(this.counted), Boolean.valueOf(isDone)});
         }

         return !isDone && !MultiPartitionPager.this.isExhausted()?this.fetchSubPage(currentCounter):null;
      }

      private Flow<FlowablePartition> fetchSubPage(DataLimits.Counter currentCounter) {
         if(currentCounter != null) {
            if(this.pageSize.isInBytes()) {
               this.toQuery = PageSize.bytesSize(this.toQuery.rawSize() - currentCounter.bytesCounted());
            } else {
               this.toQuery = PageSize.rowsSize(this.toQuery.rawSize() - currentCounter.counted());
            }
         } else {
            this.toQuery = this.pageSize;
         }

         if(MultiPartitionPager.logger.isTraceEnabled()) {
            MultiPartitionPager.logger.trace("{} - fetchSubPage, subPager: [{}], pageSize: {} {}", new Object[]{Integer.valueOf(MultiPartitionPager.this.hashCode()), MultiPartitionPager.this.pagers[MultiPartitionPager.this.current].command, Integer.valueOf(this.toQuery.rawSize()), this.toQuery.isInRows()?PageSize.PageUnit.ROWS:PageSize.PageUnit.BYTES});
         }

         return this.ctx == null?MultiPartitionPager.this.pagers[MultiPartitionPager.this.current].fetchPageInternal(this.toQuery):MultiPartitionPager.this.pagers[MultiPartitionPager.this.current].fetchPage(this.toQuery, this.ctx);
      }

      private void close() {
         if(MultiPartitionPager.logger.isTraceEnabled()) {
            MultiPartitionPager.logger.trace("{} - closed, counted: {}, remaining: {}", new Object[]{Integer.valueOf(MultiPartitionPager.this.hashCode()), Integer.valueOf(this.counted), Integer.valueOf(MultiPartitionPager.this.remaining)});
         }

      }
   }
}
