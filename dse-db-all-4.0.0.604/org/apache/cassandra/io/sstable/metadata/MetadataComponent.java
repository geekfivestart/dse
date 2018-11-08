package org.apache.cassandra.io.sstable.metadata;

public abstract class MetadataComponent implements Comparable<MetadataComponent> {
   public MetadataComponent() {
   }

   public abstract MetadataType getType();

   public int compareTo(MetadataComponent o) {
      return this.getType().compareTo(o.getType());
   }
}
