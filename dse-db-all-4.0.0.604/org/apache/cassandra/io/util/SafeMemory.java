package org.apache.cassandra.io.util;

import org.apache.cassandra.utils.UnsafeMemoryAccess;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseable;

public class SafeMemory extends Memory implements SharedCloseable {
   private final Ref<?> ref;

   public SafeMemory(long size) {
      super(size);
      this.ref = new Ref((Object)null, new SafeMemory.MemoryTidy(this.peer, size));
   }

   private SafeMemory(SafeMemory copyOf) {
      super(copyOf);
      this.ref = copyOf.ref.ref();
      if(this.peer == 0L && this.size != 0L) {
         this.ref.ensureReleased();
         throw new IllegalStateException("Cannot create a sharedCopy of a SafeMemory object that has already been closed");
      }
   }

   public SafeMemory sharedCopy() {
      return new SafeMemory(this);
   }

   public void free() {
      this.ref.release();
      this.peer = 0L;
   }

   public void close() {
      this.ref.ensureReleased();
      this.peer = 0L;
   }

   public Throwable close(Throwable accumulate) {
      return this.ref.ensureReleased(accumulate);
   }

   public SafeMemory copy(long newSize) {
      SafeMemory copy = new SafeMemory(newSize);
      copy.put(0L, this, 0L, Math.min(this.size(), newSize));
      return copy;
   }

   protected void checkBounds(long start, long end) {
      assert this.peer != 0L || this.size == 0L : this.ref.printDebugInfo();

      super.checkBounds(start, end);
   }

   public void addTo(Ref.IdentityCollection identities) {
      identities.add(this.ref);
   }

   private static final class MemoryTidy implements RefCounted.Tidy {
      final long peer;
      final long size;

      private MemoryTidy(long peer, long size) {
         this.peer = peer;
         this.size = size;
      }

      public void tidy() {
         if(this.peer != 0L) {
            UnsafeMemoryAccess.free(this.peer);
         }

      }

      public String name() {
         return Memory.toString(this.peer, this.size);
      }
   }
}
