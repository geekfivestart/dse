package org.apache.cassandra.exceptions;

import java.io.IOException;
import java.util.UUID;
import org.apache.cassandra.schema.TableMetadata;

public final class UnknownIndexException extends IOException {
   public final UUID indexId;

   public UnknownIndexException(TableMetadata metadata, UUID id) {
      super(String.format("Unknown index %s for table %s", new Object[]{id.toString(), metadata.toString()}));
      this.indexId = id;
   }
}
