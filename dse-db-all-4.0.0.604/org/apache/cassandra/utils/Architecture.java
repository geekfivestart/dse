package org.apache.cassandra.utils;

import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Set;

public final class Architecture {
   private static final Set<String> UNALIGNED_ARCH = Collections.unmodifiableSet(Sets.newHashSet(new String[]{"i386", "x86", "amd64", "x86_64", "s390x", "aarch64", "ppc64le"}));
   public static final boolean IS_UNALIGNED;

   private Architecture() {
   }

   static {
      IS_UNALIGNED = UNALIGNED_ARCH.contains(System.getProperty("os.arch"));
   }
}
