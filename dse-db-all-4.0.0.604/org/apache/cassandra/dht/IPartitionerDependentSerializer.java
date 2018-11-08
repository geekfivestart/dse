package org.apache.cassandra.dht;

import java.io.DataInput;
import java.io.IOException;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.versioning.Version;

public interface IPartitionerDependentSerializer<T, V extends Enum<V> & Version<V>> {
   void serialize(T var1, DataOutputPlus var2, V var3) throws IOException;

   T deserialize(DataInput var1, IPartitioner var2, V var3) throws IOException;

   int serializedSize(T var1, V var2);
}
