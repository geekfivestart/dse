package org.apache.cassandra.io.util;

import io.netty.channel.epoll.AIOContext.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AioCoordinator {
   private static final Logger logger = LoggerFactory.getLogger(AioCoordinator.class);
   static final int maxPendingDepth = 65536;
   private final int numTPCCores;
   private final int numIOCores;
   private final int globalQueueDepth;
   private final int[] tpcToAioCores;

   public AioCoordinator(int numTPCCores, int numIOCores, int globalQueueDepth) {
      assert numIOCores >= 0 : String.format("Invalid numIOCores: %d", new Object[]{Integer.valueOf(numIOCores)});

      assert numIOCores <= numTPCCores : String.format("Expected numIOCores %d to be <= than numTPCCores %d", new Object[]{Integer.valueOf(numIOCores), Integer.valueOf(numTPCCores)});

      this.numTPCCores = numTPCCores;
      this.numIOCores = numIOCores;
      this.globalQueueDepth = globalQueueDepth;
      this.tpcToAioCores = new int[numTPCCores];
      if(numIOCores > 0) {
         this.initAioCores();
      }

      logger.debug("AIO coordinator: {}", this);
   }

   private void initAioCores() {
      int curr = 0;

      for(int i = 0; i < this.numTPCCores; ++i) {
         this.tpcToAioCores[i] = curr++;
         if(curr == this.numIOCores) {
            curr = 0;
         }
      }

   }

   public Config getIOConfig(int coreId) {
      assert coreId >= 0 && coreId < this.tpcToAioCores.length : "Invalid core id: " + coreId;

      if(this.numIOCores == 0) {
         return null;
      } else {
         int aioCore = this.tpcToAioCores[coreId];
         int depth = this.globalQueueDepth % this.numIOCores == 0?this.globalQueueDepth / this.numIOCores:(int)Math.floor((double)this.globalQueueDepth / (double)this.numIOCores) + (aioCore + 1 <= this.globalQueueDepth % this.numIOCores?1:0);
         Config ret = coreId == aioCore && depth > 0?new Config(depth, 65536):null;
         if(ret != null) {
            logger.debug("Assigning AIO [{}] to core {}", ret, Integer.valueOf(coreId));
         }

         return ret;
      }
   }

   public int getIOCore(int coreId) {
      return this.tpcToAioCores[coreId];
   }

   public String toString() {
      return String.format("Num cores: %d, Num IO cores: %d, globalQueueDepth: %d, maxPending: %d", new Object[]{Integer.valueOf(this.numTPCCores), Integer.valueOf(this.numIOCores), Integer.valueOf(this.globalQueueDepth), Integer.valueOf(65536)});
   }
}
