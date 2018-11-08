package org.apache.cassandra.db.partitions;

import java.util.NoSuchElementException;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;

public class SingletonUnfilteredPartitionIterator implements UnfilteredPartitionIterator {
   private final UnfilteredRowIterator iter;
   private boolean returned;

   public SingletonUnfilteredPartitionIterator(UnfilteredRowIterator iter) {
      this.iter = iter;
   }

   public TableMetadata metadata() {
      return this.iter.metadata();
   }

   public boolean hasNext() {
      return !this.returned;
   }

   public UnfilteredRowIterator next() {
      if(this.returned) {
         throw new NoSuchElementException();
      } else {
         this.returned = true;
         return this.iter;
      }
   }

   public void remove() {
      throw new UnsupportedOperationException();
   }

   public void close() {
      if(!this.returned) {
         this.iter.close();
      }

   }
}
