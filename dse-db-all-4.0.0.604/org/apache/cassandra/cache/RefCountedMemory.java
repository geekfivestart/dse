package org.apache.cassandra.cache;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.cassandra.io.util.Memory;

public class RefCountedMemory extends Memory implements AutoCloseable {
   private volatile int references = 1;
   private static final AtomicIntegerFieldUpdater<RefCountedMemory> referencesUpdater = AtomicIntegerFieldUpdater.newUpdater(RefCountedMemory.class, "references");

   public RefCountedMemory(long size) {
      super(size);
   }

   public boolean reference() {
      int n;
      do {
         n = referencesUpdater.get(this);
         if(n <= 0) {
            return false;
         }
      } while(!referencesUpdater.compareAndSet(this, n, n + 1));

      return true;
   }

   public void unreference() {
      if(referencesUpdater.decrementAndGet(this) == 0) {
         super.free();
      }

   }

   public RefCountedMemory copy(long newSize) {
      RefCountedMemory copy = new RefCountedMemory(newSize);
      copy.put(0L, this, 0L, Math.min(this.size(), newSize));
      return copy;
   }

   public void free() {
      throw new AssertionError();
   }

   public void close() {
      this.unreference();
   }
}
