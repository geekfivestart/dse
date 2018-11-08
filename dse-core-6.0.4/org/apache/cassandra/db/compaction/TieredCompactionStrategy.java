package org.apache.cassandra.db.compaction;

import com.datastax.bdp.cassandra.db.tiered.AggregateDirectories;
import com.datastax.bdp.cassandra.db.tiered.TieredSSTableMultiWriter;
import com.datastax.bdp.cassandra.db.tiered.TieredStorageConfig;
import com.datastax.bdp.cassandra.db.tiered.TieredStorageConfigurations;
import com.datastax.bdp.cassandra.db.tiered.TieredStorageStrategy;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ImmutableSet.Builder;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TieredCompactionStrategy extends AbstractCompactionStrategy {
   private static final Logger logger = LoggerFactory.getLogger(TieredCompactionStrategy.class);
   public static final String CONFIG = "config";
   public static final String TIERING_STRATEGY = "tiering_strategy";
   private final TieredStorageStrategy strategy;
   private final Set<SSTableReader> orphans = new HashSet();
   private volatile boolean orphanWarningThrown = false;

   public TieredCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options) {
      super(cfs, options);
      this.strategy = createStorageStrategy(cfs, options);
   }

   private static TieredStorageStrategy createStorageStrategy(ColumnFamilyStore cfs, Map<String, String> options) {
      String configName = (String)options.get("config");

      try {
         TieredStorageConfig config = TieredStorageConfigurations.getConfigOrDefault(configName, cfs.metadata.keyspace, cfs.metadata.name);
         Map<String, String> localOpts = config.applyLocalOptions(options);
         Class<? extends TieredStorageStrategy> klass = TieredStorageStrategy.getKlass((String)localOpts.get("tiering_strategy"));
         Constructor<? extends TieredStorageStrategy> ctor = klass.getConstructor(new Class[]{ColumnFamilyStore.class, String.class, TieredStorageConfig.class, Map.class});
         return (TieredStorageStrategy)ctor.newInstance(new Object[]{cfs, configName, config, cleanStrategyOptions(localOpts)});
      } catch (InvocationTargetException | IllegalAccessException | InstantiationException | NoSuchMethodException var7) {
         throw Throwables.cleaned(var7);
      }
   }

   private static Map<String, String> cleanStrategyOptions(Map<String, String> options) {
      Map<String, String> copy = new HashMap(options);
      copy.remove("config");
      copy.remove("tiering_strategy");
      return copy;
   }

   public AbstractCompactionTask getCompactionTask(LifecycleTransaction txn, int gcBefore, long maxSSTableBytes) {
      SSTableReader sstable = (SSTableReader)Iterables.get(txn.originals(), 0);
      TieredStorageStrategy.Tier tier = this.strategy.manages(sstable.getFilename());
      if(tier == null) {
         tier = this.strategy.getDefaultTier();
      }

      AbstractCompactionTask task = super.getCompactionTask(txn, gcBefore, maxSSTableBytes);
      return this.wrapTask(tier, task);
   }

   public void addSSTable(SSTableReader sstable) {
      TieredStorageStrategy.Tier tier = this.strategy.manages(sstable.getFilename());
      if(tier != null) {
         tier.getCompactionStrategy().addSSTable(sstable);
      } else {
         Set var3 = this.orphans;
         synchronized(this.orphans) {
            if(!this.orphanWarningThrown) {
               logger.warn("{}.{} is using tiered storage, but has at least one sstable in a location not managed by it's configured tiers. These sstables will eventually be moved into a tier during the normal compaction process, but you can force this process to begin immediately by running 'nodetool compact {} {}'", new Object[]{this.cfs.metadata.keyspace, this.cfs.metadata.name, this.cfs.metadata.keyspace, this.cfs.metadata.name});
               this.orphanWarningThrown = true;
            }

            this.orphans.add(sstable);
         }

         this.strategy.getDefaultTier().getCompactionStrategy().addSSTable(sstable);
      }
   }

   public void removeSSTable(SSTableReader sstable) {
      Iterator var2 = this.strategy.getTiers().iterator();

      while(var2.hasNext()) {
         TieredStorageStrategy.Tier tier = (TieredStorageStrategy.Tier)var2.next();
         tier.getCompactionStrategy().removeSSTable(sstable);
      }

      Set var6 = this.orphans;
      synchronized(this.orphans) {
         this.orphans.remove(sstable);
      }
   }

   protected Set<SSTableReader> getSSTables() {
      Builder<SSTableReader> builder = ImmutableSet.builder();
      Iterator var2 = this.strategy.getTiers().iterator();

      while(var2.hasNext()) {
         TieredStorageStrategy.Tier tier = (TieredStorageStrategy.Tier)var2.next();
         builder.addAll(tier.getCompactionStrategy().getSSTables());
      }

      builder.addAll(this.orphans);
      return builder.build();
   }

   public int getEstimatedRemainingTasks() {
      int remaining = 0;

      TieredStorageStrategy.Tier tier;
      for(Iterator var2 = this.strategy.getTiers().iterator(); var2.hasNext(); remaining += tier.getCompactionStrategy().getEstimatedRemainingTasks()) {
         tier = (TieredStorageStrategy.Tier)var2.next();
      }

      return remaining;
   }

   public long getMaxSSTableBytes() {
      long maxBytes = -9223372036854775808L;

      TieredStorageStrategy.Tier tier;
      for(Iterator var3 = this.strategy.getTiers().iterator(); var3.hasNext(); maxBytes = Math.max(maxBytes, tier.getCompactionStrategy().getMaxSSTableBytes())) {
         tier = (TieredStorageStrategy.Tier)var3.next();
      }

      return maxBytes;
   }

   private CompactionTask wrapTask(TieredStorageStrategy.Tier tier, AbstractCompactionTask task) {
      assert task instanceof CompactionTask;

      return new TieredCompactionTaskWrapper(this.strategy, tier, (CompactionTask)task);
   }

   private Collection<AbstractCompactionTask> wrapTasks(TieredStorageStrategy.Tier tier, Collection<AbstractCompactionTask> tasks) {
      List<AbstractCompactionTask> wrapped = new ArrayList(tasks.size());
      Iterator var4 = tasks.iterator();

      while(var4.hasNext()) {
         AbstractCompactionTask task = (AbstractCompactionTask)var4.next();
         wrapped.add(this.wrapTask(tier, task));
      }

      return wrapped;
   }

   private AbstractCompactionTask maybeGetRelocationTask(int gcBefore) {
      Set var2 = this.orphans;
      synchronized(this.orphans) {
         if(this.orphans.isEmpty()) {
            return null;
         } else {
            Iterator var3 = this.orphans.iterator();

            while(var3.hasNext()) {
               SSTableReader sstable = (SSTableReader)var3.next();
               if(!sstable.isMarkedCompacted()) {
                  LifecycleTransaction modifier = this.cfs.getTracker().tryModify(Collections.singleton(sstable), OperationType.COMPACTION);
                  if(modifier != null) {
                     return new RelocationCompactionTask(this.cfs, modifier, gcBefore, this.strategy);
                  }
               }
            }

            return null;
         }
      }
   }

   public AbstractCompactionTask getNextBackgroundTask(int gcBefore) {
      Iterator var2 = this.strategy.getTiers().iterator();

      TieredStorageStrategy.Tier tier;
      AbstractCompactionTask task;
      do {
         if(!var2.hasNext()) {
            return this.maybeGetRelocationTask(gcBefore);
         }

         tier = (TieredStorageStrategy.Tier)var2.next();
         task = tier.getCompactionStrategy().getNextBackgroundTask(gcBefore);
      } while(task == null);

      return this.wrapTask(tier, task);
   }

   public Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput) {
      List<AbstractCompactionTask> tasks = new LinkedList();
      Iterator var4 = this.strategy.getTiers().iterator();

      while(var4.hasNext()) {
         TieredStorageStrategy.Tier tier = (TieredStorageStrategy.Tier)var4.next();
         Collection<AbstractCompactionTask> maximalTasks = tier.getCompactionStrategy().getMaximalTask(gcBefore, splitOutput);
         if(maximalTasks != null && !maximalTasks.isEmpty()) {
            tasks.addAll(this.wrapTasks(tier, maximalTasks));
         }
      }

      AbstractCompactionTask relocationTask = this.maybeGetRelocationTask(gcBefore);
      if(relocationTask != null) {
         tasks.add(relocationTask);
      }

      return tasks;
   }

   public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> collection, int i) {
      throw new UnsupportedOperationException();
   }

   public static Map<String, String> validateStrategy(Map<String, String> options) throws ConfigurationException {
      Class klass = TieredStorageStrategy.getKlass((String)options.get("tiering_strategy"));

      try {
         return (Map)klass.getMethod("validateOptions", new Class[]{Map.class}).invoke((Object)null, new Object[]{options});
      } catch (NoSuchMethodException var4) {
         return options;
      } catch (InvocationTargetException var5) {
         if(var5.getTargetException() instanceof ConfigurationException) {
            throw (ConfigurationException)var5.getTargetException();
         } else {
            Throwable cause = var5.getCause() == null?var5:var5.getCause();
            throw new ConfigurationException(String.format("%s.validateOptions() threw an error for: %s %s", new Object[]{klass.getName(), cause.getClass().getName(), ((Throwable)cause).getMessage()}), var5);
         }
      } catch (IllegalAccessException var6) {
         throw new ConfigurationException("Cannot access method validateOptions in " + klass.getName(), var6);
      }
   }

   public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException {
      boolean checkingThresholdSupport = false;
      StackTraceElement[] var2 = Thread.currentThread().getStackTrace();
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         StackTraceElement ste = var2[var4];
         if(ste.getClassName().endsWith(CompactionParams.class.getSimpleName()) && ste.getMethodName().equals("supportsThresholdParams")) {
            checkingThresholdSupport = true;
            break;
         }
      }

      Map<String, String> unchecked = new HashMap(options);
      if(!checkingThresholdSupport) {
         if(!options.containsKey("config")) {
            throw new ConfigurationException(String.format("The compaction option '%s' must be specified for %s" + options.toString(), new Object[]{"config", TieredCompactionStrategy.class.getSimpleName()}));
         }

         if(TieredStorageConfigurations.getConfig((String)options.get("config")) == null) {
            throw new ConfigurationException(String.format("The tiered storage configuration '%s' does not exist", new Object[]{options.get("config")}));
         }

         if(!options.containsKey("tiering_strategy")) {
            throw new ConfigurationException(String.format("The compaction option '%s' must be specified for %s" + options.toString(), new Object[]{"tiering_strategy", TieredCompactionStrategy.class.getSimpleName()}));
         }

         unchecked = validateStrategy(options);
      }

      ((Map)unchecked).remove("config");
      ((Map)unchecked).remove("tiering_strategy");
      return (Map)unchecked;
   }

   public TieredStorageStrategy getStorageStrategy() {
      return this.strategy;
   }

   public Directories getDirectories() {
      return new AggregateDirectories(this.strategy);
   }

   public boolean supportsEarlyOpen() {
      return false;
   }

   public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, MetadataCollector meta, SerializationHeader header, Collection<Index> indexes, LifecycleTransaction txn) {
      return new TieredSSTableMultiWriter(this.cfs, keyCount, repairedAt, pendingRepair, meta, header, indexes, txn, this.strategy);
   }
}
