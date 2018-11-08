package org.apache.cassandra.io.sstable.metadata;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputPlus;

public interface IMetadataSerializer {
   void serialize(Map<MetadataType, MetadataComponent> var1, DataOutputPlus var2, Version var3) throws IOException;

   Map<MetadataType, MetadataComponent> deserialize(Descriptor var1, EnumSet<MetadataType> var2) throws IOException;

   MetadataComponent deserialize(Descriptor var1, MetadataType var2) throws IOException;

   void mutateLevel(Descriptor var1, int var2) throws IOException;

   void mutateRepaired(Descriptor var1, long var2, UUID var4) throws IOException;
}
