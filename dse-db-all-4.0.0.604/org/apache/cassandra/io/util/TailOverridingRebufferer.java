package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

public class TailOverridingRebufferer extends WrappingRebufferer {
   private final long cutoff;
   private final ByteBuffer tail;

   public TailOverridingRebufferer(Rebufferer source, long cutoff, ByteBuffer tail) {
      super(source);
      this.cutoff = cutoff;
      this.tail = tail;
   }

   public Rebufferer.BufferHolder rebuffer(long position, Rebufferer.ReaderConstraint constraint) {
      if(position < this.cutoff) {
         WrappingRebufferer.WrappingBufferHolder ret = (WrappingRebufferer.WrappingBufferHolder)super.rebuffer(position, constraint);
         if(ret.offset() + (long)ret.limit() > this.cutoff) {
            ret.limit((int)(this.cutoff - ret.offset()));
         }

         return ret;
      } else {
         return this.newBufferHolder().initialize((Rebufferer.BufferHolder)null, this.tail.duplicate(), this.cutoff);
      }
   }

   public long fileLength() {
      return this.cutoff + (long)this.tail.limit();
   }

   public String paramsToString() {
      return String.format("+%d@%d", new Object[]{Integer.valueOf(this.tail.limit()), Long.valueOf(this.cutoff)});
   }
}
