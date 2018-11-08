package org.apache.cassandra.db.rows;

import com.google.common.hash.Hasher;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.HashingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RowIterators {
   private static final Logger logger = LoggerFactory.getLogger(RowIterators.class);

   private RowIterators() {
   }

   public static void digest(RowIterator iterator, Hasher hasher) {
      HashingUtils.updateBytes(hasher, iterator.partitionKey().getKey().duplicate());
      iterator.columns().regulars.digest(hasher);
      iterator.columns().statics.digest(hasher);
      HashingUtils.updateWithBoolean(hasher, iterator.isReverseOrder());
      iterator.staticRow().digest(hasher);

      while(iterator.hasNext()) {
         ((Row)iterator.next()).digest(hasher);
      }

   }

   public static RowIterator withOnlyQueriedData(RowIterator iterator, ColumnFilter filter) {
      return filter.allFetchedColumnsAreQueried()?iterator:Transformation.apply((RowIterator)iterator, new WithOnlyQueriedData(filter));
   }

   public static RowIterator loggingIterator(RowIterator iterator, final String id) {
      final TableMetadata metadata = iterator.metadata();
      logger.info("[{}] Logging iterator on {}.{}, partition key={}, reversed={}", new Object[]{id, metadata.keyspace, metadata.name, metadata.partitionKeyType.getString(iterator.partitionKey().getKey()), Boolean.valueOf(iterator.isReverseOrder())});
      class Log extends Transformation {
         Log() {
         }

         public Row applyToStatic(Row row) {
            if(!row.isEmpty()) {
               RowIterators.logger.info("[{}] {}", id, row.toString(metadata));
            }

            return row;
         }

         public Row applyToRow(Row row) {
            RowIterators.logger.info("[{}] {}", id, row.toString(metadata));
            return row;
         }
      }

      return Transformation.apply((RowIterator)iterator, new Log());
   }
}
