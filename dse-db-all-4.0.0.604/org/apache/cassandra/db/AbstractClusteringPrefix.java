package org.apache.cassandra.db;

import com.google.common.hash.Hasher;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.cassandra.utils.HashingUtils;

public abstract class AbstractClusteringPrefix implements ClusteringPrefix {
   public AbstractClusteringPrefix() {
   }

   public ClusteringPrefix clustering() {
      return this;
   }

   public int dataSize() {
      int size = 0;

      for(int i = 0; i < this.size(); ++i) {
         ByteBuffer bb = this.get(i);
         size += bb == null?0:bb.remaining();
      }

      return size;
   }

   public void digest(Hasher hasher) {
      for(int i = 0; i < this.size(); ++i) {
         ByteBuffer bb = this.get(i);
         if(bb != null) {
            HashingUtils.updateBytes(hasher, bb.duplicate());
         }
      }

      HashingUtils.updateWithByte(hasher, this.kind().ordinal());
   }

   public final int hashCode() {
      int result = 31;

      for(int i = 0; i < this.size(); ++i) {
         result += 31 * Objects.hashCode(this.get(i));
      }

      return 31 * result + Objects.hashCode(this.kind());
   }

   public final boolean equals(Object o) {
      if(!(o instanceof ClusteringPrefix)) {
         return false;
      } else {
         ClusteringPrefix that = (ClusteringPrefix)o;
         if(this.kind() == that.kind() && this.size() == that.size()) {
            for(int i = 0; i < this.size(); ++i) {
               if(!Objects.equals(this.get(i), that.get(i))) {
                  return false;
               }
            }

            return true;
         } else {
            return false;
         }
      }
   }
}
