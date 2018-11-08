package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.transport.ProtocolVersion;

public interface MultiCellType {
   AbstractType<?> nameComparator();

   ByteBuffer serializeForNativeProtocol(Iterator<Cell> var1, ProtocolVersion var2);
}
