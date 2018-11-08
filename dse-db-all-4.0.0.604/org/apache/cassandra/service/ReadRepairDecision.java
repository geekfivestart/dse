package org.apache.cassandra.service;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.cassandra.schema.TableMetadata;

public enum ReadRepairDecision {
   NONE,
   GLOBAL,
   DC_LOCAL;

   private ReadRepairDecision() {
   }

   public static ReadRepairDecision newDecision(TableMetadata metadata) {
      if(metadata.params.readRepairChance > 0.0D || metadata.params.dcLocalReadRepairChance > 0.0D) {
         double chance = ThreadLocalRandom.current().nextDouble();
         if(metadata.params.readRepairChance > chance) {
            return GLOBAL;
         }

         if(metadata.params.dcLocalReadRepairChance > chance) {
            return DC_LOCAL;
         }
      }

      return NONE;
   }
}
