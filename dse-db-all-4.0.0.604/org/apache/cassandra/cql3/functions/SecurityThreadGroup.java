package org.apache.cassandra.cql3.functions;

import java.util.Set;

public final class SecurityThreadGroup extends ThreadGroup {
   private final Set<String> allowedPackages;
   private final SecurityThreadGroup.ThreadInitializer threadInitializer;

   public SecurityThreadGroup(String name, Set<String> allowedPackages, SecurityThreadGroup.ThreadInitializer threadInitializer) {
      super(name);
      this.allowedPackages = allowedPackages;
      this.threadInitializer = threadInitializer;
   }

   public void initializeThread() {
      this.threadInitializer.initializeThread();
   }

   public boolean isPackageAllowed(String pkg) {
      return this.allowedPackages == null || this.allowedPackages.contains(pkg);
   }

   @FunctionalInterface
   interface ThreadInitializer {
      void initializeThread();
   }
}
