package com.datastax.bdp.cassandra.db.tiered;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Directories.DataDirectory;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

public class TieredStorageConfig {
   public final Map<String, String> local_options;
   public final List<TieredStorageConfig.Tier> tiers;

   public static TieredStorageConfig fromYamlConfig(TieredStorageYamlConfig config) {
      return new TieredStorageConfig(config);
   }

   private TieredStorageConfig(TieredStorageYamlConfig yamlConfig) {
      this.local_options = ImmutableMap.copyOf(yamlConfig.local_options);
      List<TieredStorageConfig.Tier> tempTiers = new ArrayList(yamlConfig.tiers.size());
      Iterator var3 = yamlConfig.tiers.iterator();

      while(var3.hasNext()) {
         TieredStorageYamlConfig.Tier yamlTier = (TieredStorageYamlConfig.Tier)var3.next();
         tempTiers.add(new TieredStorageConfig.Tier(yamlTier));
      }

      this.tiers = ImmutableList.copyOf(tempTiers);
   }

   static Class<? extends AbstractCompactionStrategy> getCompactionKlass(String name, String tierSetName, int tier) {
      String className = name.contains(".")?name:"org.apache.cassandra.db.compaction." + name;
      Class<AbstractCompactionStrategy> strategyClass = FBUtilities.classForName(className, "compaction strategy");
      if(!AbstractCompactionStrategy.class.isAssignableFrom(strategyClass)) {
         throw new ConfigurationException(String.format("Compaction strategy class %s, specified for tier %s of tier set %s, does not extend from AbstractReplicationStrategy", new Object[]{className, Integer.valueOf(tier), tierSetName}));
      } else {
         return strategyClass;
      }
   }

   public void validate(String tierSetName) {
      int tierNum = 0;

      for(Iterator var3 = this.tiers.iterator(); var3.hasNext(); ++tierNum) {
         TieredStorageConfig.Tier tier = (TieredStorageConfig.Tier)var3.next();
         TieredStorageStrategy.validateTierOptions(tierNum, tier.paths);
      }

   }

   public Map<String, String> applyLocalOptions(Map<String, String> ddlOpts) {
      Map<String, String> options = new HashMap(ddlOpts);
      Iterator var3 = this.local_options.entrySet().iterator();

      while(var3.hasNext()) {
         Entry<String, String> entry = (Entry)var3.next();
         options.put(entry.getKey(), entry.getValue());
      }

      return options;
   }

   public static class Tier {
      public final List<String> paths;

      public Tier(TieredStorageYamlConfig.Tier yamlTier) {
         List<String> tempPaths = new ArrayList(yamlTier.paths.size());
         Iterator var3 = yamlTier.paths.iterator();

         while(var3.hasNext()) {
            String path = (String)var3.next();
            File tierDirectory = new File(path);

            try {
               tempPaths.add(tierDirectory.getCanonicalPath());
            } catch (IOException var7) {
               throw new RuntimeException(var7);
            }
         }

         this.paths = ImmutableList.copyOf(tempPaths);
      }

      public DataDirectory[] createDataDirectories() {
         DataDirectory[] directories = new DataDirectory[this.paths.size()];

         for(int i = 0; i < this.paths.size(); ++i) {
            directories[i] = new DataDirectory(new File((String)this.paths.get(i)));
         }

         return directories;
      }

      public Directories createDirectories(TableMetadata cfm) {
         return new Directories(cfm, this.createDataDirectories());
      }
   }
}
