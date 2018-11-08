package org.apache.cassandra.db.rows;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;

public abstract class LazilyInitializedUnfilteredRowIterator extends AbstractIterator<Unfiltered> implements UnfilteredRowIterator {
   private final DecoratedKey partitionKey;
   private UnfilteredRowIterator iterator;

   public LazilyInitializedUnfilteredRowIterator(DecoratedKey partitionKey) {
      this.partitionKey = partitionKey;
   }

   protected abstract UnfilteredRowIterator initializeIterator();

   protected void maybeInit() {
      if(this.iterator == null) {
         this.iterator = this.initializeIterator();
      }

   }

   public boolean initialized() {
      return this.iterator != null;
   }

   public TableMetadata metadata() {
      this.maybeInit();
      return this.iterator.metadata();
   }

   public RegularAndStaticColumns columns() {
      this.maybeInit();
      return this.iterator.columns();
   }

   public boolean isReverseOrder() {
      this.maybeInit();
      return this.iterator.isReverseOrder();
   }

   public DecoratedKey partitionKey() {
      return this.partitionKey;
   }

   public DeletionTime partitionLevelDeletion() {
      this.maybeInit();
      return this.iterator.partitionLevelDeletion();
   }

   public Row staticRow() {
      this.maybeInit();
      return this.iterator.staticRow();
   }

   public EncodingStats stats() {
      this.maybeInit();
      return this.iterator.stats();
   }

   protected Unfiltered computeNext() {
      this.maybeInit();
      return this.iterator.hasNext()?(Unfiltered)this.iterator.next():(Unfiltered)this.endOfData();
   }

   public void close() {
      if(this.iterator != null) {
         this.iterator.close();
      }

   }
}
