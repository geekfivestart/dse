package org.apache.cassandra.db;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiskBoundaryManager {
   private static final boolean SPLIT_SSTABLES_BY_TOKEN_RANGE = Boolean.parseBoolean(System.getProperty("cassandra.split_sstables_by_token_range", "true"));
   private static final Logger logger = LoggerFactory.getLogger(DiskBoundaryManager.class);
   private ConcurrentHashMap<Directories, DiskBoundaries> diskBoundaries = new ConcurrentHashMap();

   public DiskBoundaryManager() {
   }

   public DiskBoundaries getDiskBoundaries(ColumnFamilyStore cfs, Directories directories) {
      if(cfs.getPartitioner().splitter().isPresent() && SPLIT_SSTABLES_BY_TOKEN_RANGE) {
         DiskBoundaries boundaries = (DiskBoundaries)this.diskBoundaries.get(directories);
         if(boundaries == null || boundaries.isOutOfDate()) {
            synchronized(this) {
               boundaries = (DiskBoundaries)this.diskBoundaries.get(directories);
               if(boundaries == null || boundaries.isOutOfDate()) {
                  logger.trace("Refreshing disk boundary cache for {}.{}", cfs.keyspace.getName(), cfs.getTableName());
                  DiskBoundaries oldBoundaries = boundaries;
                  boundaries = getDiskBoundaryValue(cfs, directories);
                  this.diskBoundaries.put(directories, boundaries);
                  logger.debug("Updating boundaries from {} to {} for {}.{}", new Object[]{oldBoundaries, boundaries, cfs.keyspace.getName(), cfs.getTableName()});
               }
            }
         }

         return boundaries;
      } else {
         return new DiskBoundaries(cfs.getDirectories().getWriteableLocations(), BlacklistedDirectories.getDirectoriesVersion());
      }
   }

   private static DiskBoundaries getDiskBoundaryValue(ColumnFamilyStore cfs, Directories directories) {
      Object localRanges;
      long ringVersion;
      TokenMetadata tmd;
      do {
         tmd = StorageService.instance.getTokenMetadata();
         ringVersion = tmd.getRingVersion();
         if(StorageService.instance.isBootstrapMode() && !StorageService.isReplacingSameAddress()) {
            PendingRangeCalculatorService.instance.blockUntilFinished();
            localRanges = tmd.getPendingRanges(cfs.keyspace.getName(), FBUtilities.getBroadcastAddress());
         } else {
            localRanges = cfs.keyspace.getReplicationStrategy().getAddressRanges(tmd.cloneAfterAllSettled()).get(FBUtilities.getBroadcastAddress());
         }

         logger.trace("Got local ranges {} (ringVersion = {})", localRanges, Long.valueOf(ringVersion));
      } while(ringVersion != tmd.getRingVersion());

      int directoriesVersion;
      Directories.DataDirectory[] dirs;
      do {
         directoriesVersion = BlacklistedDirectories.getDirectoriesVersion();
         dirs = directories.getWriteableLocations();
      } while(directoriesVersion != BlacklistedDirectories.getDirectoriesVersion());

      if(localRanges != null && !((Collection)localRanges).isEmpty()) {
         List<Range<Token>> sortedLocalRanges = Range.sort((Collection)localRanges);
         return createDiskBoundaries(cfs, directories.getWriteableLocations(), sortedLocalRanges);
      } else {
         return new DiskBoundaries(dirs, (List)null, ringVersion, directoriesVersion);
      }
   }

   @VisibleForTesting
   public static DiskBoundaries createDiskBoundaries(ColumnFamilyStore cfs, Directories.DataDirectory[] dirs, List<Range<Token>> sortedLocalRanges) {
      List<PartitionPosition> positions = getDiskBoundaries(sortedLocalRanges, cfs.getPartitioner(), dirs);
      return new DiskBoundaries(dirs, positions, StorageService.instance.getTokenMetadata().getRingVersion(), BlacklistedDirectories.getDirectoriesVersion());
   }

   private static List<PartitionPosition> getDiskBoundaries(List<Range<Token>> sortedLocalRanges, IPartitioner partitioner, Directories.DataDirectory[] dataDirectories) {
      assert partitioner.splitter().isPresent();

      Splitter splitter = (Splitter)partitioner.splitter().get();
      boolean dontSplitRanges = DatabaseDescriptor.getNumTokens() > 1;
      List<Token> boundaries = splitter.splitOwnedRanges(dataDirectories.length, sortedLocalRanges, dontSplitRanges);
      if(dontSplitRanges && boundaries.size() < dataDirectories.length) {
         boundaries = splitter.splitOwnedRanges(dataDirectories.length, sortedLocalRanges, false);
      }

      List<PartitionPosition> diskBoundaries = new ArrayList();

      for(int i = 0; i < boundaries.size() - 1; ++i) {
         diskBoundaries.add(((Token)boundaries.get(i)).maxKeyBound());
      }

      diskBoundaries.add(partitioner.getMaximumToken().maxKeyBound());
      return diskBoundaries;
   }

   public void invalidate() {
      this.diskBoundaries.forEach((k, v) -> {
         v.invalidate();
      });
      this.diskBoundaries.clear();
   }
}
