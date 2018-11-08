package com.datastax.bdp.cassandra.db.tiered;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Directories.DataDirectory;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TieredStorageStrategy {
   private static final Logger logger = LoggerFactory.getLogger(TieredStorageStrategy.class);
   protected final ColumnFamilyStore cfs;
   protected final String name;
   protected final List<TieredStorageStrategy.Tier> tiers;
   protected final TieredStorageStrategy.Tier defaultTier;

   public TieredStorageStrategy(ColumnFamilyStore cfs, String name, TieredStorageConfig config, Map<String, String> options) {
      this.cfs = cfs;
      this.name = name;
      this.tiers = ImmutableList.copyOf(this.createTiers(config, options));
      this.defaultTier = this.findDefaultTier();
   }

   protected abstract List<TieredStorageStrategy.Tier> createTiers(TieredStorageConfig var1, Map<String, String> var2);

   protected TieredStorageStrategy.Tier findDefaultTier() {
      return (TieredStorageStrategy.Tier)this.tiers.get(this.tiers.size() - 1);
   }

   public TieredStorageStrategy.Tier getDefaultTier() {
      return this.defaultTier;
   }

   protected abstract TieredStorageStrategy.Context newContext();

   public int getTierForRow(Unfiltered row, TieredStorageStrategy.Context ctx) {
      int idx = 0;

      for(Iterator var4 = this.tiers.iterator(); var4.hasNext(); ++idx) {
         TieredStorageStrategy.Tier tier = (TieredStorageStrategy.Tier)var4.next();
         if(tier.applies(row, ctx)) {
            return idx;
         }
      }

      return this.defaultTier.getLevel();
   }

   public TieredStorageStrategy.Tier manages(String path) {
      Iterator var2 = this.getTiers().iterator();

      TieredStorageStrategy.Tier tier;
      do {
         if(!var2.hasNext()) {
            return null;
         }

         tier = (TieredStorageStrategy.Tier)var2.next();
      } while(!tier.managesPath(path));

      return tier;
   }

   public int getTierForDeletion(DeletionTime deletion, TieredStorageStrategy.Context ctx) {
      int idx = 0;

      for(Iterator var4 = this.tiers.iterator(); var4.hasNext(); ++idx) {
         TieredStorageStrategy.Tier tier = (TieredStorageStrategy.Tier)var4.next();
         if(tier.applies(deletion, ctx)) {
            return idx;
         }
      }

      return this.defaultTier.getLevel();
   }

   public List<TieredStorageStrategy.Tier> getTiers() {
      return this.tiers;
   }

   public TieredStorageStrategy.Tier getTier(int level) {
      return (TieredStorageStrategy.Tier)this.tiers.get(level);
   }

   public ColumnFamilyStore getCfs() {
      return this.cfs;
   }

   public DataDirectory[] getWritableLocations() {
      Set<DataDirectory> directorySet = new HashSet();
      Iterator var2 = this.getTiers().iterator();

      while(var2.hasNext()) {
         TieredStorageStrategy.Tier tier = (TieredStorageStrategy.Tier)var2.next();
         directorySet.addAll(Sets.newHashSet(tier.getDirectories().getWriteableLocations()));
      }

      DataDirectory[] dataDirectories = new DataDirectory[directorySet.size()];
      directorySet.toArray(dataDirectories);
      Arrays.sort(dataDirectories, (o1, o2) -> {
         return o1.location.compareTo(o2.location);
      });
      return dataDirectories;
   }

   public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException {
      return new HashMap(options);
   }

   public static void validateTierOptions(int tier, List<String> paths) throws ConfigurationException {
      if(paths != null && !paths.isEmpty()) {
         Iterator var2 = paths.iterator();

         String path;
         File f;
         do {
            if(!var2.hasNext()) {
               return;
            }

            path = (String)var2.next();
            f = new File(path);
            if(!f.exists() && !f.mkdirs()) {
               throw new ConfigurationException(String.format("Path '%s' provided for tier %s does not exist", new Object[]{path, Integer.valueOf(tier)}));
            }
         } while(f.isDirectory());

         throw new ConfigurationException(String.format("Path '%s' provided for tier %s is not a directory", new Object[]{path, Integer.valueOf(tier)}));
      } else {
         throw new ConfigurationException(String.format("No paths provided for tier %s", new Object[]{Integer.valueOf(tier)}));
      }
   }

   public static Class<? extends TieredStorageStrategy> getKlass(String name) {
      assert name != null;

      String className = name.contains(".")?name:"com.datastax.bdp.cassandra.db.tiered." + name;
      Class<TieredStorageStrategy> strategyClass = FBUtilities.classForName(className, "compaction strategy");
      if(!TieredStorageStrategy.class.isAssignableFrom(strategyClass)) {
         throw new ConfigurationException(String.format("Tiered storage strategy class %s, does not extend TieredStorageStrategy", new Object[]{className}));
      } else {
         return strategyClass;
      }
   }

   public abstract class Tier {
      protected final int level;
      protected final List<String> managedPaths;
      protected final Directories directories;
      protected final DataDirectory[] dataDirectories;
      protected final AbstractCompactionStrategy compactionStrategy;

      public Tier(int this$0, TieredStorageConfig.Tier level, Map<String, String> config) {
         this.level = level;
         this.dataDirectories = config.createDataDirectories();
         this.directories = config.createDirectories(TieredStorageStrategy.this.cfs.metadata());
         this.compactionStrategy = this.createCompactionStrategy(options);
         this.managedPaths = config.paths;
         TieredStorageStrategy.logger.debug("Tier ({}) created at level {} with paths: {}", new Object[]{this.getClass().getName(), Integer.valueOf(level), this.managedPaths});
      }

      private AbstractCompactionStrategy createCompactionStrategy(Map<String, String> options) {
         Map<String, String> combinedOpts = new HashMap(this.getDefaultCompactionOptions());
         combinedOpts.putAll(options);
         CompactionParams params = CompactionParams.create(this.getDefaultCompactionClass(), this.cleanCompactionOptions(combinedOpts));
         params.validate();
         return TieredStorageStrategy.this.cfs.createCompactionStrategyInstance(params);
      }

      protected Map<String, String> cleanCompactionOptions(Map<String, String> options) {
         return options;
      }

      protected abstract Class<? extends AbstractCompactionStrategy> getDefaultCompactionClass();

      protected Map<String, String> getDefaultCompactionOptions() {
         return Collections.emptyMap();
      }

      public abstract boolean applies(Unfiltered var1, TieredStorageStrategy.Context var2);

      public abstract boolean applies(DeletionTime var1, TieredStorageStrategy.Context var2);

      public Directories getDirectories() {
         return this.directories;
      }

      public Collection<DataDirectory> getDataDirectories() {
         return Sets.newHashSet(this.dataDirectories);
      }

      public int getLevel() {
         return this.level;
      }

      public boolean managesPath(String filename) {
         File file = new File(filename);

         try {
            Iterator var3 = this.managedPaths.iterator();

            String path;
            do {
               if(!var3.hasNext()) {
                  return false;
               }

               path = (String)var3.next();
            } while(!file.getCanonicalPath().startsWith(path));

            return true;
         } catch (IOException var5) {
            throw new RuntimeException(var5);
         }
      }

      public AbstractCompactionStrategy getCompactionStrategy() {
         return this.compactionStrategy;
      }
   }

   public interface Context {
   }
}
