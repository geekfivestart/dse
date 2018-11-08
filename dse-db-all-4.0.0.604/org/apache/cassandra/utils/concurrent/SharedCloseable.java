package org.apache.cassandra.utils.concurrent;

public interface SharedCloseable extends AutoCloseable {
   SharedCloseable sharedCopy();

   Throwable close(Throwable var1);

   void addTo(Ref.IdentityCollection var1);
}
