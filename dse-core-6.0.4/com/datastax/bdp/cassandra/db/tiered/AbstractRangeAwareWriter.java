package com.datastax.bdp.cassandra.db.tiered;

import java.util.List;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.Directories.DataDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRangeAwareWriter {
   private static final Logger logger = LoggerFactory.getLogger(AbstractRangeAwareWriter.class);
   private int currentIndex = -1;

   public AbstractRangeAwareWriter() {
   }

   protected abstract DataDirectory[] getLocations();

   protected abstract List<PartitionPosition> getBoundaries();

   protected abstract void switchWriteLocation(DataDirectory var1);

   void maybeSwitchWriter(DecoratedKey key) {
      List<PartitionPosition> boundaries = this.getBoundaries();
      DataDirectory[] locations = this.getLocations();
      int prevIdx = this.currentIndex;

      boolean shouldSwitch;
      for(shouldSwitch = false; this.currentIndex < 0 || boundaries != null && key.compareTo((PartitionPosition)boundaries.get(this.currentIndex)) > 0; ++this.currentIndex) {
         shouldSwitch = true;
      }

      if(shouldSwitch) {
         if(prevIdx >= 0) {
            logger.debug("Switching write location from {} to {}", locations[prevIdx], locations[this.currentIndex]);
         }

         this.switchWriteLocation(locations[this.currentIndex]);
      }

   }
}
