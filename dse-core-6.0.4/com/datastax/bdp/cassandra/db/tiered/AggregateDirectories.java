package com.datastax.bdp.cassandra.db.tiered;

import java.io.File;
import java.util.Arrays;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Directories.DataDirectory;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class AggregateDirectories extends Directories {
   private final TieredStorageStrategy strategy;
   private final AggregateDirectories.RepresentativeDirectory representativeDirectory;

   public AggregateDirectories(TieredStorageStrategy strategy) {
      super(strategy.cfs.metadata(), strategy.getWritableLocations());
      this.strategy = strategy;
      this.representativeDirectory = new AggregateDirectories.RepresentativeDirectory(strategy);
   }

   public DataDirectory[] getWriteableLocations() {
      return new DataDirectory[]{this.representativeDirectory};
   }

   public File getWriteableLocationAsFile(long writeSize) {
      return this.strategy.getTier(0).getDirectories().getWriteableLocationAsFile(writeSize);
   }

   public DataDirectory getWriteableLocation(long writeSize) {
      return writeSize <= this.representativeDirectory.getAvailableSpace()?this.representativeDirectory:null;
   }

   public File getWriteableLocationAsFile(ColumnFamilyStore cfs, SSTableReader original, long writeSize) {
      TieredStorageStrategy.Tier tier = this.strategy.manages(original.getFilename());
      if(tier == null) {
         tier = this.strategy.getDefaultTier();
      }

      return tier.getDirectories().getWriteableLocationAsFile(cfs, original, writeSize);
   }

   private static class RepresentativeDirectory extends DataDirectory {
      private final TieredStorageStrategy strategy;
      private final DataDirectory[] paths;

      public RepresentativeDirectory(TieredStorageStrategy strategy) {
         super(strategy.getWritableLocations()[0].location);
         this.strategy = strategy;
         this.paths = strategy.getWritableLocations();
      }

      public long getAvailableSpace() {
         long total = 0L;
         DataDirectory[] var3 = this.paths;
         int var4 = var3.length;

         for(int var5 = 0; var5 < var4; ++var5) {
            DataDirectory directory = var3[var5];
            total += directory.getAvailableSpace();
         }

         return total;
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            AggregateDirectories.RepresentativeDirectory that = (AggregateDirectories.RepresentativeDirectory)o;
            return this.strategy.cfs.metadata.id.equals(that.strategy.cfs.metadata.id) && Arrays.equals(this.paths, that.paths);
         } else {
            return false;
         }
      }

      public int hashCode() {
         int result = this.strategy.cfs.metadata.id.hashCode();
         result = 31 * result + Arrays.hashCode(this.paths);
         return result;
      }
   }
}
