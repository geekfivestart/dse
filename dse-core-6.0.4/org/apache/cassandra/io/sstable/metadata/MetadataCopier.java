package org.apache.cassandra.io.sstable.metadata;

import org.apache.cassandra.schema.TableMetadata;

public class MetadataCopier {
   public MetadataCopier() {
   }

   public static MetadataCollector copy(TableMetadata metaData, MetadataCollector src) {
      MetadataCollector dst = (new MetadataCollector(metaData.comparator)).commitLogIntervals(src.commitLogIntervals).sstableLevel(src.sstableLevel);
      return dst;
   }
}
