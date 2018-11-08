package org.apache.cassandra.exceptions;

import org.apache.cassandra.schema.TableId;

public class UnknownTableException extends InternalRequestExecutionException {
   public final TableId id;

   public UnknownTableException(TableId id) {
      this("Cannot find table with ID " + id, id);
   }

   public UnknownTableException(String msg, TableId id) {
      super(RequestFailureReason.UNKNOWN_TABLE, msg);
      this.id = id;
   }
}
