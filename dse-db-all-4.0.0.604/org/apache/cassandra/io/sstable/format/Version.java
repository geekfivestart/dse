package org.apache.cassandra.io.sstable.format;

import org.apache.cassandra.db.EncodingVersion;

public abstract class Version {
   protected final String version;
   protected final SSTableFormat format;

   protected Version(SSTableFormat format, String version) {
      this.format = format;
      this.version = version;
   }

   public abstract boolean isLatestVersion();

   public abstract EncodingVersion encodingVersion();

   public abstract boolean hasCommitLogLowerBound();

   public abstract boolean hasCommitLogIntervals();

   public abstract boolean hasMaxCompressedLength();

   public abstract boolean hasPendingRepair();

   public abstract boolean hasMetadataChecksum();

   public String getVersion() {
      return this.version;
   }

   public SSTableFormat getSSTableFormat() {
      return this.format;
   }

   public abstract boolean isCompatible();

   public abstract boolean isCompatibleForStreaming();

   public String toString() {
      return this.version;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         Version version1 = (Version)o;
         if(this.version != null) {
            if(!this.version.equals(version1.version)) {
               return false;
            }
         } else if(version1.version != null) {
            return false;
         }

         return true;
      } else {
         return false;
      }
   }

   public int hashCode() {
      return this.version != null?this.version.hashCode():0;
   }
}
