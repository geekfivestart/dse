package org.apache.cassandra.db.rows;

public interface RowIterator extends BaseRowIterator<Row> {
   default boolean isEmpty() {
      return this.staticRow().isEmpty() && !this.hasNext();
   }
}
