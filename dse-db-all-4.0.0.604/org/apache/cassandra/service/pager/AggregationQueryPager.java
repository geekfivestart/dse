package org.apache.cassandra.service.pager;

import io.reactivex.functions.Function;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ReadContext;
import org.apache.cassandra.db.aggregation.GroupingState;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.flow.Flow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AggregationQueryPager implements QueryPager {
   private static final Logger logger = LoggerFactory.getLogger(AggregationQueryPager.class);
   private static final PageSize MIN_SUB_PAGE_SIZE = PageSize.bytesSize(2097152);
   private static final int DEFAULT_GROUPS = 100;
   private final DataLimits limits;
   private QueryPager subPager;
   private final long timeout;
   private final int minSubPageSizeRows;

   public AggregationQueryPager(QueryPager subPager, DataLimits limits, long timeoutMillis, int avgRowSize) {
      this.subPager = subPager;
      this.limits = limits;
      this.timeout = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
      this.minSubPageSizeRows = MIN_SUB_PAGE_SIZE.inEstimatedRows(avgRowSize);
   }

   public Flow<FlowablePartition> fetchPage(PageSize pageSize, ReadContext ctx) {
      return this.limits.isGroupByLimit()?(new AggregationQueryPager.GroupByPartitions(pageSize, ctx)).partitions():(new AggregationQueryPager.AggregatedPartitions(pageSize, ctx)).partitions();
   }

   public Flow<FlowablePartition> fetchPageInternal(PageSize pageSize) {
      return this.limits.isGroupByLimit()?(new AggregationQueryPager.GroupByPartitions(pageSize, (ReadContext)null)).partitions():(new AggregationQueryPager.AggregatedPartitions(pageSize, (ReadContext)null)).partitions();
   }

   public boolean isExhausted() {
      return this.subPager.isExhausted();
   }

   public int maxRemaining() {
      return this.subPager.maxRemaining();
   }

   public PagingState state(boolean inclusive) {
      return this.subPager.state(inclusive);
   }

   public QueryPager withUpdatedLimit(DataLimits newLimits) {
      throw new UnsupportedOperationException();
   }

   private final class AggregatedPartitions extends AggregationQueryPager.GroupByPartitions {
      AggregatedPartitions(PageSize pageSize, ReadContext ctx) {
         super(pageSize, ctx);
      }

      protected QueryPager updatePagerLimit(QueryPager pager, DataLimits limits, ByteBuffer lastPartitionKey, Clustering lastClustering) {
         return pager;
      }

      protected boolean isDone(int pageSize, int counted) {
         return false;
      }

      protected int computeSubPageSize(int pageSize, int counted) {
         return Math.max(AggregationQueryPager.this.minSubPageSizeRows, pageSize);
      }
   }

   class GroupByPartitions {
      private final PageSize pageSize;
      private final int topPages;
      @Nullable
      private final ReadContext ctx;
      private ByteBuffer lastPartitionKey;
      private Clustering lastClustering;
      private int initialMaxRemaining;

      GroupByPartitions(PageSize pageSize, ReadContext ctx) {
         this.pageSize = pageSize;
         this.topPages = this.handlePageSize(pageSize);
         this.ctx = ctx;
         if(AggregationQueryPager.logger.isTraceEnabled()) {
            AggregationQueryPager.logger.trace("{} - created with page size={}, ctx={}", new Object[]{Integer.valueOf(this.hashCode()), Integer.valueOf(this.topPages), ctx});
         }

      }

      Flow<FlowablePartition> partitions() {
         this.initialMaxRemaining = AggregationQueryPager.this.subPager.maxRemaining();
         Flow<FlowablePartition> ret = this.fetchSubPage(this.topPages);
         return ret.concatWith(this::moreContents).map(this::applyToPartition);
      }

      private FlowablePartition applyToPartition(FlowablePartition partition) {
         this.checkTimeout();
         this.lastPartitionKey = partition.partitionKey().getKey();
         this.lastClustering = null;
         return partition.mapContent(this::applyToRow);
      }

      private Row applyToRow(Row row) {
         this.checkTimeout();
         this.lastClustering = row.clustering();
         return row;
      }

      private int handlePageSize(PageSize pageSize) {
         int size = pageSize.isInBytes()?100:pageSize.rawSize();
         return size <= 0?2147483647:size;
      }

      private Flow<FlowablePartition> moreContents() {
         int counted = this.initialMaxRemaining - AggregationQueryPager.this.subPager.maxRemaining();
         if(AggregationQueryPager.logger.isTraceEnabled()) {
            AggregationQueryPager.logger.trace("{} - moreContents() called with last: {}/{}, counted: {}", new Object[]{Integer.valueOf(this.hashCode()), this.lastPartitionKey == null?"null":ByteBufferUtil.bytesToHex(this.lastPartitionKey), this.lastClustering == null?"null":this.lastClustering.toBinaryString(), Integer.valueOf(counted)});
         }

         if(!this.isDone(this.topPages, counted) && !AggregationQueryPager.this.subPager.isExhausted()) {
            AggregationQueryPager.this.subPager = this.updatePagerLimit(AggregationQueryPager.this.subPager, AggregationQueryPager.this.limits, this.lastPartitionKey, this.lastClustering);
            return this.fetchSubPage(this.computeSubPageSize(this.topPages, counted));
         } else {
            if(AggregationQueryPager.logger.isTraceEnabled()) {
               AggregationQueryPager.logger.trace("{} - moreContents() returns null: {}, {}, [{}] exhausted? {}", new Object[]{Integer.valueOf(this.hashCode()), Integer.valueOf(counted), Integer.valueOf(this.topPages), Integer.valueOf(AggregationQueryPager.this.subPager.hashCode()), Boolean.valueOf(AggregationQueryPager.this.subPager.isExhausted())});
            }

            return null;
         }
      }

      protected boolean isDone(int pageSize, int counted) {
         return counted == pageSize;
      }

      protected void checkTimeout() {
         if(this.ctx != null) {
            long elapsed = System.nanoTime() - this.ctx.queryStartNanos;
            if(elapsed > AggregationQueryPager.this.timeout) {
               throw new ReadTimeoutException(this.ctx.consistencyLevel);
            }
         }
      }

      protected QueryPager updatePagerLimit(QueryPager pager, DataLimits limits, ByteBuffer lastPartitionKey, Clustering lastClustering) {
         GroupingState state = new GroupingState(lastPartitionKey, lastClustering);
         DataLimits newLimits = limits.forGroupByInternalPaging(state);
         return pager.withUpdatedLimit(newLimits);
      }

      protected int computeSubPageSize(int pageSize, int counted) {
         return pageSize - counted;
      }

      private Flow<FlowablePartition> fetchSubPage(int subPageSize) {
         if(AggregationQueryPager.logger.isTraceEnabled()) {
            AggregationQueryPager.logger.trace("Fetching sub-page with consistency {}", this.ctx == null?"<internal>":this.ctx.consistencyLevel);
         }

         return this.ctx == null?AggregationQueryPager.this.subPager.fetchPageInternal(PageSize.rowsSize(subPageSize)):AggregationQueryPager.this.subPager.fetchPage(PageSize.rowsSize(subPageSize), this.ctx.withStartTime(System.nanoTime()));
      }
   }
}
