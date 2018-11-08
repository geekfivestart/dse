package org.apache.cassandra.exceptions;

import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class UnknownColumnException extends InternalRequestExecutionException {
   public final ByteBuffer columnName;

   public UnknownColumnException(TableMetadata metadata, ByteBuffer columnName) {
      super(RequestFailureReason.UNKNOWN_COLUMN, String.format("Unknown column %s in table %s", new Object[]{stringify(columnName), metadata}));
      this.columnName = columnName;
   }

   private static String stringify(ByteBuffer name) {
      try {
         return (new ColumnIdentifier(name, UTF8Type.instance)).toCQLString();
      } catch (Exception var2) {
         return ByteBufferUtil.bytesToHex(name);
      }
   }
}
