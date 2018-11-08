package org.apache.cassandra.db;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageService;

public class DiskBoundaries {
   public final List<Directories.DataDirectory> directories;
   public final ImmutableList<PartitionPosition> positions;
   final long ringVersion;
   final int directoriesVersion;
   private volatile boolean isInvalid;

   public DiskBoundaries(Directories.DataDirectory[] directories, int diskVersion) {
      this(directories, (List)null, -1L, diskVersion);
   }

   @VisibleForTesting
   public DiskBoundaries(Directories.DataDirectory[] directories, List<PartitionPosition> positions, long ringVersion, int diskVersion) {
      this.isInvalid = false;
      this.directories = directories == null?null:Collections.unmodifiableList(Lists.newArrayList(directories));
      this.positions = positions == null?null:ImmutableList.copyOf(positions);
      this.ringVersion = ringVersion;
      this.directoriesVersion = diskVersion;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         DiskBoundaries that = (DiskBoundaries)o;
         return this.ringVersion != that.ringVersion?false:(this.directoriesVersion != that.directoriesVersion?false:(!this.directories.equals(that.directories)?false:(this.positions != null?this.positions.equals(that.positions):that.positions == null)));
      } else {
         return false;
      }
   }

   public int hashCode() {
      int result = this.directories != null?this.directories.hashCode():0;
      result = 31 * result + (this.positions != null?this.positions.hashCode():0);
      result = 31 * result + (int)(this.ringVersion ^ this.ringVersion >>> 32);
      result = 31 * result + this.directoriesVersion;
      return result;
   }

   public String toString() {
      return "DiskBoundaries{directories=" + this.directories + ", positions=" + this.positions + ", ringVersion=" + this.ringVersion + ", directoriesVersion=" + this.directoriesVersion + '}';
   }

   public boolean isOutOfDate() {
      if(this.isInvalid) {
         return true;
      } else {
         int currentDiskVersion = BlacklistedDirectories.getDirectoriesVersion();
         long currentRingVersion = StorageService.instance.getTokenMetadata().getRingVersion();
         return currentDiskVersion != this.directoriesVersion || this.ringVersion != -1L && currentRingVersion != this.ringVersion;
      }
   }

   public void invalidate() {
      this.isInvalid = true;
   }

   public int getDiskIndex(SSTableReader sstable) {
      if(this.positions == null) {
         return this.getBoundariesFromSSTableDirectory(sstable.descriptor);
      } else {
         int pos = Collections.binarySearch(this.positions, sstable.first);

         assert pos < 0;

         return -pos - 1;
      }
   }

   public int getBoundariesFromSSTableDirectory(Descriptor descriptor) {
      for(int i = 0; i < this.directories.size(); ++i) {
         Directories.DataDirectory directory = (Directories.DataDirectory)this.directories.get(i);
         if(descriptor.directory.getAbsolutePath().startsWith(directory.location.getAbsolutePath())) {
            return i;
         }
      }

      return 0;
   }

   public Directories.DataDirectory getCorrectDiskForSSTable(SSTableReader sstable) {
      return (Directories.DataDirectory)this.directories.get(this.getDiskIndex(sstable));
   }
}
