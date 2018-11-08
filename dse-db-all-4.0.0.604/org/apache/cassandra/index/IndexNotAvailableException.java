package org.apache.cassandra.index;

import org.apache.cassandra.exceptions.InternalRequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailureReason;

public final class IndexNotAvailableException extends InternalRequestExecutionException {
   public IndexNotAvailableException(Index index) {
      super(RequestFailureReason.INDEX_NOT_AVAILABLE, String.format("The secondary index '%s' is not yet available", new Object[]{index.getIndexMetadata().name}));
   }
}
