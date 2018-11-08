package com.datastax.bdp.cassandra.db.tiered;

import com.datastax.bdp.config.DseConfig;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories.DataDirectory;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TieredStorageConfigurations {
   private static final Logger logger = LoggerFactory.getLogger(TieredStorageConfigurations.class);
   public static final TieredStorageConfigurations instance = new TieredStorageConfigurations();
   @VisibleForTesting
   static Map<String, TieredStorageConfig> tierSets = new HashMap();
   private static volatile boolean initialized = false;
   static final TieredStorageConfig DEFAULT;

   public TieredStorageConfigurations() {
   }

   private static void updateCFDirectories(TieredStorageConfig options) {
      Iterator var1 = options.tiers.iterator();

      while(var1.hasNext()) {
         TieredStorageConfig.Tier tier = (TieredStorageConfig.Tier)var1.next();
         DataDirectory[] directories = new DataDirectory[tier.paths.size()];

         for(int i = 0; i < tier.paths.size(); ++i) {
            directories[i] = new DataDirectory(new File((String)tier.paths.get(i)));
         }

         ColumnFamilyStore.addInitialDirectories(directories);
      }

   }

   public static synchronized void checkConfig() throws ConfigurationException {
      if(!initialized) {
         Set<String> tierNames = new HashSet();
         Iterator var1 = DseConfig.getTieredStorageOptions().entrySet().iterator();

         while(var1.hasNext()) {
            Entry<String, TieredStorageConfig> entry = (Entry)var1.next();
            String name = (String)entry.getKey();
            TieredStorageConfig config = (TieredStorageConfig)entry.getValue();
            if(tierNames.contains(name)) {
               throw new ConfigurationException(String.format("Tiered storage config '%s' already exists", new Object[]{name}));
            }

            config.validate(name);
            tierNames.add(name);
         }

      }
   }

   public static synchronized void init() throws ConfigurationException {
      if(!initialized) {
         Iterator var0 = DseConfig.getTieredStorageOptions().entrySet().iterator();

         while(var0.hasNext()) {
            Entry<String, TieredStorageConfig> entry = (Entry)var0.next();
            registerConfig((String)entry.getKey(), (TieredStorageConfig)entry.getValue());
         }

         initialized = true;
      }
   }

   public static synchronized void registerConfig(String name, TieredStorageConfig config) throws ConfigurationException {
      if(tierSets.containsKey(name)) {
         throw new IllegalStateException(String.format("Tiered storage config '%s' already exists", new Object[]{name}));
      } else {
         config.validate(name);
         updateCFDirectories(config);
         tierSets.put(name, config);
      }
   }

   public static synchronized TieredStorageConfig getConfig(String name) {
      assert initialized;

      return (TieredStorageConfig)tierSets.get(name);
   }

   public static synchronized TieredStorageConfig getConfigOrDefault(String name, String keyspace, String table) {
      TieredStorageConfig config = getConfig(name);
      if(config == null) {
         logger.warn("Unable to find tiered config {} while creating compaction strategy for {}.{}\r\nA generic config which uses the default data directories will be used in it's place. Please add the tiered config '{}' to dse.yaml and restart DSE", new Object[]{name, keyspace, table, name});
         return DEFAULT;
      } else {
         return config;
      }
   }

   static {
      TieredStorageYamlConfig yamlConfig = new TieredStorageYamlConfig();
      String[] paths = DatabaseDescriptor.getAllDataFileLocations();
      TieredStorageYamlConfig.Tier tier = new TieredStorageYamlConfig.Tier();
      String[] var3 = paths;
      int var4 = paths.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         String path = var3[var5];
         tier.paths.add(path);
      }

      yamlConfig.tiers.add(tier);
      DEFAULT = TieredStorageConfig.fromYamlConfig(yamlConfig);
   }
}
