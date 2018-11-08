package org.apache.cassandra.io.sstable.metadata;

import java.io.IOException;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public interface IMetadataComponentSerializer<T extends MetadataComponent> {
   int serializedSize(Version var1, T var2) throws IOException;

   void serialize(Version var1, T var2, DataOutputPlus var3) throws IOException;

   T deserialize(Version var1, DataInputPlus var2) throws IOException;
}
