package org.apache.cassandra.io.sstable.metadata;

import org.apache.cassandra.db.SerializationHeader;

public enum MetadataType {
   VALIDATION(ValidationMetadata.serializer),
   COMPACTION(CompactionMetadata.serializer),
   STATS(StatsMetadata.serializer),
   HEADER(SerializationHeader.serializer);

   public final IMetadataComponentSerializer<MetadataComponent> serializer;

   private MetadataType(IMetadataComponentSerializer<MetadataComponent> serializer) {
      this.serializer = serializer;
   }
}
