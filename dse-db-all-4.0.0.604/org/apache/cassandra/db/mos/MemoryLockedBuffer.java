package org.apache.cassandra.db.mos;

public class MemoryLockedBuffer {
   public final long address;
   public final long amount;
   public final boolean succeeded;

   MemoryLockedBuffer(long address, long amount, boolean succeeded) {
      this.address = address;
      this.amount = amount;
      this.succeeded = succeeded;
   }

   public long locked() {
      return this.succeeded?this.amount:0L;
   }

   public long notLocked() {
      return this.succeeded?0L:this.amount;
   }

   public static MemoryLockedBuffer succeeded(long address, long amount) {
      return new MemoryLockedBuffer(address, amount, true);
   }

   public static MemoryLockedBuffer failed(long address, long amount) {
      return new MemoryLockedBuffer(address, amount, false);
   }
}
