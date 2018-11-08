package org.apache.cassandra.service.pager;

import io.reactivex.functions.Action;
import java.util.function.Function;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadContext;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.flow.Flow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractQueryPager<T extends ReadCommand> implements QueryPager {
   private static final Logger logger = LoggerFactory.getLogger(AbstractQueryPager.class);
   protected final T command;
   protected final ProtocolVersion protocolVersion;
   private final boolean enforceStrictLiveness;
   private AbstractQueryPager.Pager internalPager;
   private int remaining;
   protected boolean inclusive;
   public DecoratedKey lastKey;
   private int remainingInPartition;
   private boolean exhausted;
   DataLimits.Counter lastCounter;

   protected AbstractQueryPager(T command, ProtocolVersion protocolVersion) {
      this.command = command;
      this.protocolVersion = protocolVersion;
      this.remaining = command.limits().count();
      this.remainingInPartition = command.limits().perPartitionCount();
      this.enforceStrictLiveness = command.metadata().enforceStrictLiveness();
      if(logger.isTraceEnabled()) {
         logger.trace("{} - created with {}/{}/{}", new Object[]{Integer.valueOf(this.hashCode()), command.limits(), Integer.valueOf(this.remaining), Integer.valueOf(this.remainingInPartition)});
      }

   }

   public Flow<FlowablePartition> fetchPage(PageSize pageSize, ReadContext ctx) {
      return this.innerFetch(pageSize, (pageCommand) -> {
         return pageCommand.execute(ctx);
      });
   }

   public Flow<FlowablePartition> fetchPageInternal(PageSize pageSize) {
      return this.innerFetch(pageSize, ReadQuery::executeInternal);
   }

   public Flow<FlowableUnfilteredPartition> fetchPageUnfiltered(PageSize pageSize) {
      assert this.internalPager == null : "only one iteration at a time is supported";

      if(this.isExhausted()) {
         return Flow.empty();
      } else {
         ReadCommand pageCommand = this.nextPageReadCommand(this.nextPageLimits(), pageSize);
         if(pageCommand == null) {
            this.exhausted = true;
            return Flow.empty();
         } else {
            this.internalPager = new AbstractQueryPager.UnfilteredPager(pageCommand.limits().duplicate(), this.command.nowInSec());
            return ((AbstractQueryPager.UnfilteredPager)this.internalPager).apply(pageCommand.executeLocally());
         }
      }
   }

   private Flow<FlowablePartition> innerFetch(PageSize pageSize, Function<ReadCommand, Flow<FlowablePartition>> dataSupplier) {
      assert this.internalPager == null : "only one iteration at a time is supported";

      if(this.isExhausted()) {
         return Flow.empty();
      } else {
         ReadCommand pageCommand = this.nextPageReadCommand(this.nextPageLimits(), pageSize);
         if(pageCommand == null) {
            this.exhausted = true;
            return Flow.empty();
         } else {
            this.internalPager = new AbstractQueryPager.RowPager(pageCommand.limits().duplicate(), this.command.nowInSec());
            return ((AbstractQueryPager.RowPager)this.internalPager).apply((Flow)dataSupplier.apply(pageCommand));
         }
      }
   }

   private DataLimits nextPageLimits() {
      return this.limits().withCount(Math.min(this.limits().count(), this.remaining));
   }

   void restoreState(DecoratedKey lastKey, int remaining, int remainingInPartition, boolean inclusive) {
      if(logger.isTraceEnabled()) {
         logger.trace("{} - restoring state: {}/{}/{}/{}", new Object[]{Integer.valueOf(this.hashCode()), lastKey, Integer.valueOf(remaining), Integer.valueOf(remainingInPartition), Boolean.valueOf(inclusive)});
      }

      this.lastKey = lastKey;
      this.remaining = remaining;
      this.remainingInPartition = remainingInPartition;
      this.inclusive = inclusive;
   }

   public int counted() {
      return this.internalPager != null?this.internalPager.counter.counted():0;
   }

   public boolean isExhausted() {
      return this.exhausted || this.limitsExceeded();
   }

   private boolean limitsExceeded() {
      return this.remaining == 0 || this instanceof SinglePartitionPager && this.remainingInPartition == 0;
   }

   public PagingState state(boolean inclusive) {
      return this.internalPager != null?this.makePagingState(this.lastKey, this.internalPager.lastRow, inclusive):this.makePagingState(inclusive);
   }

   public int maxRemaining() {
      return this.internalPager == null?this.remaining:this.internalPager.getRemaining();
   }

   int remainingInPartition() {
      return this.internalPager == null?this.remainingInPartition:this.internalPager.getRemainingInPartition();
   }

   DataLimits limits() {
      return this.command.limits();
   }

   protected abstract PagingState makePagingState(DecoratedKey var1, Row var2, boolean var3);

   protected abstract PagingState makePagingState(boolean var1);

   protected abstract ReadCommand nextPageReadCommand(DataLimits var1, PageSize var2);

   protected abstract void recordLast(DecoratedKey var1, Row var2);

   protected abstract boolean isPreviouslyReturnedPartition(DecoratedKey var1);

   private class Pager<T extends Unfiltered> {
      protected final DataLimits pageLimits;
      protected final DataLimits.Counter counter;
      protected DecoratedKey currentKey;
      protected Row lastRow;
      protected boolean isFirstPartition = true;

      protected Pager(DataLimits pageLimits, int nowInSec) {
         this.counter = pageLimits.newCounter(nowInSec, true, AbstractQueryPager.this.command.selectsFullPartition(), AbstractQueryPager.this.enforceStrictLiveness);
         this.pageLimits = pageLimits;
      }

      protected void onClose() {
         if(AbstractQueryPager.logger.isTraceEnabled()) {
            AbstractQueryPager.logger.trace("{} - onClose called with {}/{}", new Object[]{Integer.valueOf(AbstractQueryPager.this.hashCode()), AbstractQueryPager.this.lastKey, this.lastRow});
         }

         this.counter.endOfIteration();
         AbstractQueryPager.this.recordLast(AbstractQueryPager.this.lastKey, this.lastRow);
         AbstractQueryPager.this.remaining = this.getRemaining();
         AbstractQueryPager.this.remainingInPartition = this.getRemainingInPartition();
         AbstractQueryPager.this.exhausted = this.pageLimits.isExhausted(this.counter);
         if(AbstractQueryPager.logger.isTraceEnabled()) {
            AbstractQueryPager.logger.trace("{} - exhausted {}, counter: {}, remaining: {}/{}", new Object[]{Integer.valueOf(AbstractQueryPager.this.hashCode()), Boolean.valueOf(AbstractQueryPager.this.exhausted), this.counter, Integer.valueOf(AbstractQueryPager.this.remaining), Integer.valueOf(AbstractQueryPager.this.remainingInPartition)});
         }

         AbstractQueryPager.this.internalPager = null;
         AbstractQueryPager.this.lastCounter = this.counter;
      }

      private int getRemaining() {
         return AbstractQueryPager.this.remaining - this.counter.counted();
      }

      private int getRemainingInPartition() {
         return this.lastRow != null && this.lastRow.clustering().size() == 0?0:AbstractQueryPager.this.remainingInPartition - this.counter.countedInCurrentPartition();
      }

      protected Row applyToStatic(Row row) {
         if(!row.isEmpty()) {
            if(!this.currentKey.equals(AbstractQueryPager.this.lastKey)) {
               AbstractQueryPager.this.remainingInPartition = this.pageLimits.perPartitionCount();
            }

            AbstractQueryPager.this.lastKey = this.currentKey;
            this.lastRow = row;
         }

         return row;
      }

      protected Row applyToRow(Row row) {
         if(!this.currentKey.equals(AbstractQueryPager.this.lastKey)) {
            AbstractQueryPager.this.remainingInPartition = this.pageLimits.perPartitionCount();
            AbstractQueryPager.this.lastKey = this.currentKey;
         }

         this.lastRow = row;
         return row;
      }
   }

   private class RowPager extends AbstractQueryPager<T>.Pager<Row> {
      private RowPager(DataLimits pageLimits, int nowInSec) {
         super(pageLimits, nowInSec);
      }

      Flow<FlowablePartition> apply(Flow<FlowablePartition> source) {
         return source.flatMap((partition) -> {
            if(AbstractQueryPager.this.internalPager.isFirstPartition) {
               AbstractQueryPager.this.internalPager.isFirstPartition = false;
               if(AbstractQueryPager.this.isPreviouslyReturnedPartition(partition.partitionKey()) && !AbstractQueryPager.this.inclusive) {
                  return partition.content().skipMapEmpty((c) -> {
                     return this.applyToPartition(partition.withContent(c));
                  });
               }
            }

            return Flow.just(this.applyToPartition(partition));
         }).doOnClose(this::onClose);
      }

      private FlowablePartition applyToPartition(FlowablePartition partition) {
         this.currentKey = partition.partitionKey();
         this.applyToStatic(partition.staticRow());
         return DataLimits.truncateFiltered(this.counter, partition.mapContent(this::applyToRow));
      }
   }

   private class UnfilteredPager extends AbstractQueryPager<T>.Pager<Unfiltered> {
      private UnfilteredPager(DataLimits pageLimits, int nowInSec) {
         super(pageLimits, nowInSec);
      }

      Flow<FlowableUnfilteredPartition> apply(Flow<FlowableUnfilteredPartition> source) {
         return source.flatMap((partition) -> {
            if(AbstractQueryPager.this.internalPager.isFirstPartition) {
               AbstractQueryPager.this.internalPager.isFirstPartition = false;
               if(AbstractQueryPager.this.isPreviouslyReturnedPartition(partition.partitionKey()) && !AbstractQueryPager.this.inclusive) {
                  return partition.content().skipMapEmpty((c) -> {
                     return this.applyToPartition(partition.withContent(c));
                  });
               }
            }

            return Flow.just(this.applyToPartition(partition));
         }).doOnClose(this::onClose);
      }

      private FlowableUnfilteredPartition applyToPartition(FlowableUnfilteredPartition partition) {
         if(AbstractQueryPager.logger.isTraceEnabled()) {
            AbstractQueryPager.logger.trace("{} - applyToPartition {}", Integer.valueOf(AbstractQueryPager.this.hashCode()), ByteBufferUtil.bytesToHex(partition.partitionKey().getKey()));
         }

         this.currentKey = partition.partitionKey();
         this.applyToStatic(partition.staticRow());
         return DataLimits.truncateUnfiltered(this.counter, partition.mapContent((unfiltered) -> {
            return (Unfiltered)(unfiltered instanceof Row?this.applyToRow((Row)unfiltered):unfiltered);
         }));
      }
   }
}
