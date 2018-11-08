package com.datastax.bdp.config;

import com.datastax.bdp.util.DseUtil;
import org.apache.cassandra.utils.FBUtilities;

public class DseConstants {
   public static final int MIN_DISK_SPACE_BEFORE_FAILURE = 10;
   public static final int HEALTH_CHECK_INTERVAL = 5;
   public static final int THREADS_PER_CPU_CORE = DseUtil.calculateThreadsPerCore();
   public static final int NUM_PHYSICAL_CPU_CORES;

   public DseConstants() {
   }

   static {
      NUM_PHYSICAL_CPU_CORES = Math.max(2, FBUtilities.getAvailableProcessors() / THREADS_PER_CPU_CORE);
   }
}
