package org.apache.cassandra.schema;

import org.github.jamm.Unmetered;

@Unmetered
public final class TableMetadataRef {
   public final TableId id;
   public final String keyspace;
   public final String name;
   private volatile TableMetadata metadata;

   TableMetadataRef(TableMetadata metadata) {
      this.metadata = metadata;
      this.id = metadata.id;
      this.keyspace = metadata.keyspace;
      this.name = metadata.name;
   }

   public static TableMetadataRef forOfflineTools(TableMetadata metadata) {
      return new TableMetadataRef(metadata);
   }

   public TableMetadata get() {
      return this.metadata;
   }

   void set(TableMetadata metadata) {
      this.get().validateCompatibility(metadata);
      this.metadata = metadata;
   }

   public String toString() {
      return this.get().toString();
   }
}
