package com.datastax.bdp.cassandra.db.tiered;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TieredStorageYamlConfig {
   public List<TieredStorageYamlConfig.Tier> tiers = new ArrayList();
   public Map<String, String> local_options = new HashMap();

   public TieredStorageYamlConfig() {
   }

   public static class Tier {
      public List<String> paths = new ArrayList();

      public Tier() {
      }
   }
}
