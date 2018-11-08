package org.apache.cassandra.io.sstable.format.trieindex;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.TailOverridingRebufferer;

class PartitionIndexEarly extends PartitionIndex {
   final long cutoff;
   final ByteBuffer tail;

   public PartitionIndexEarly(FileHandle fh, long trieRoot, long keyCount, DecoratedKey first, DecoratedKey last, long cutoff, ByteBuffer tail) {
      super(fh, trieRoot, keyCount, first, last);
      this.cutoff = cutoff;
      this.tail = tail;
   }

   protected Rebufferer instantiateRebufferer() {
      return new TailOverridingRebufferer(super.instantiateRebufferer(), this.cutoff, this.tail);
   }
}
