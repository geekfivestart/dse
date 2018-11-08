package org.apache.cassandra.db.rows;

import io.reactivex.functions.Function;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.flow.Flow;

public interface FlowablePartitionBase<T> extends PartitionTrait {
   PartitionHeader header();

   Flow<T> content();

   Row staticRow();

   FlowablePartitionBase<T> withHeader(PartitionHeader var1, Row var2);

   FlowablePartitionBase<T> withContent(Flow<T> var1);

   FlowablePartitionBase<T> mapContent(Function<T, T> var1);

   FlowablePartitionBase<T> skippingMapContent(Function<T, T> var1, Row var2);

   default TableMetadata metadata() {
      return this.header().metadata;
   }

   default boolean isReverseOrder() {
      return this.header().isReverseOrder;
   }

   default RegularAndStaticColumns columns() {
      return this.header().columns;
   }

   default DecoratedKey partitionKey() {
      return this.header().partitionKey;
   }

   default DeletionTime partitionLevelDeletion() {
      return this.header().partitionLevelDeletion;
   }

   default EncodingStats stats() {
      return this.header().stats;
   }
}
