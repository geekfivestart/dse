package org.apache.cassandra.utils.concurrent;

public abstract class SharedCloseableImpl implements SharedCloseable {
   final Ref<?> ref;

   public SharedCloseableImpl(RefCounted.Tidy tidy) {
      this.ref = new Ref((Object)null, tidy);
   }

   protected SharedCloseableImpl(SharedCloseableImpl copy) {
      this.ref = copy.ref.ref();
   }

   public boolean isCleanedUp() {
      return this.ref.globalCount() == 0;
   }

   public void close() {
      this.ref.ensureReleased();
   }

   public Throwable close(Throwable accumulate) {
      return this.ref.ensureReleased(accumulate);
   }

   public void addTo(Ref.IdentityCollection identities) {
      identities.add(this.ref);
   }
}
