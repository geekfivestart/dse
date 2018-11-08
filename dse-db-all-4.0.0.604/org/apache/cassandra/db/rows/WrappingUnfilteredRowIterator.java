package org.apache.cassandra.db.rows;

import com.google.common.collect.UnmodifiableIterator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.schema.TableMetadata;

public abstract class WrappingUnfilteredRowIterator extends UnmodifiableIterator<Unfiltered> implements UnfilteredRowIterator {
   protected final UnfilteredRowIterator wrapped;

   protected WrappingUnfilteredRowIterator(UnfilteredRowIterator wrapped) {
      this.wrapped = wrapped;
   }

   public TableMetadata metadata() {
      return this.wrapped.metadata();
   }

   public RegularAndStaticColumns columns() {
      return this.wrapped.columns();
   }

   public boolean isReverseOrder() {
      return this.wrapped.isReverseOrder();
   }

   public DecoratedKey partitionKey() {
      return this.wrapped.partitionKey();
   }

   public DeletionTime partitionLevelDeletion() {
      return this.wrapped.partitionLevelDeletion();
   }

   public Row staticRow() {
      return this.wrapped.staticRow();
   }

   public EncodingStats stats() {
      return this.wrapped.stats();
   }

   public boolean hasNext() {
      return this.wrapped.hasNext();
   }

   public Unfiltered next() {
      return (Unfiltered)this.wrapped.next();
   }

   public void close() {
      this.wrapped.close();
   }
}
