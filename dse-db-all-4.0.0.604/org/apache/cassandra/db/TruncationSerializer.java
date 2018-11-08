package org.apache.cassandra.db;

import java.io.IOException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Serializer;

class TruncationSerializer implements Serializer<Truncation> {
   TruncationSerializer() {
   }

   public void serialize(Truncation t, DataOutputPlus out) throws IOException {
      out.writeUTF(t.keyspace);
      out.writeUTF(t.columnFamily);
   }

   public Truncation deserialize(DataInputPlus in) throws IOException {
      String keyspace = in.readUTF();
      String columnFamily = in.readUTF();
      return new Truncation(keyspace, columnFamily);
   }

   public long serializedSize(Truncation truncation) {
      return (long)(TypeSizes.sizeof(truncation.keyspace) + TypeSizes.sizeof(truncation.columnFamily));
   }
}
