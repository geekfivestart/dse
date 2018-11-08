package org.apache.cassandra.gms;

import java.util.concurrent.atomic.AtomicInteger;

public class VersionGenerator {
   private static final AtomicInteger version = new AtomicInteger(0);

   public VersionGenerator() {
   }

   public static int getNextVersion() {
      return version.incrementAndGet();
   }
}
