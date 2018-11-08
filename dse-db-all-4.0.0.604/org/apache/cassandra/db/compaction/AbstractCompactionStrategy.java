package org.apache.cassandra.db.compaction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.SimpleSSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.CompactionParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractCompactionStrategy {
   private static final Logger logger = LoggerFactory.getLogger(AbstractCompactionStrategy.class);
   protected static final float DEFAULT_TOMBSTONE_THRESHOLD = 0.2F;
   protected static final long DEFAULT_TOMBSTONE_COMPACTION_INTERVAL = 86400L;
   protected static final boolean DEFAULT_UNCHECKED_TOMBSTONE_COMPACTION_OPTION = false;
   protected static final boolean DEFAULT_LOG_ALL_OPTION = false;
   protected static final String TOMBSTONE_THRESHOLD_OPTION = "tombstone_threshold";
   protected static final String TOMBSTONE_COMPACTION_INTERVAL_OPTION = "tombstone_compaction_interval";
   protected static final String UNCHECKED_TOMBSTONE_COMPACTION_OPTION = "unchecked_tombstone_compaction";
   protected static final String LOG_ALL_OPTION = "log_all";
   protected static final String COMPACTION_ENABLED = "enabled";
   public static final String ONLY_PURGE_REPAIRED_TOMBSTONES = "only_purge_repaired_tombstones";
   protected Map<String, String> options;
   protected final ColumnFamilyStore cfs;
   protected float tombstoneThreshold;
   protected long tombstoneCompactionInterval;
   protected boolean uncheckedTombstoneCompaction;
   protected boolean disableTombstoneCompactions = false;
   protected boolean logAll = true;
   private final Directories directories;
   protected boolean isActive = false;

   protected AbstractCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options) {
      assert cfs != null;

      this.cfs = cfs;
      this.options = ImmutableMap.copyOf(options);

      try {
         validateOptions(options);
         String optionValue = (String)options.get("tombstone_threshold");
         this.tombstoneThreshold = optionValue == null?0.2F:Float.parseFloat(optionValue);
         optionValue = (String)options.get("tombstone_compaction_interval");
         this.tombstoneCompactionInterval = optionValue == null?86400L:Long.parseLong(optionValue);
         optionValue = (String)options.get("unchecked_tombstone_compaction");
         this.uncheckedTombstoneCompaction = optionValue == null?false:Boolean.parseBoolean(optionValue);
         optionValue = (String)options.get("log_all");
         this.logAll = optionValue == null?false:Boolean.parseBoolean(optionValue);
      } catch (ConfigurationException var4) {
         logger.warn("Error setting compaction strategy options ({}), defaults will be used", var4.getMessage());
         this.tombstoneThreshold = 0.2F;
         this.tombstoneCompactionInterval = 86400L;
         this.uncheckedTombstoneCompaction = false;
      }

      this.directories = cfs.getDirectories();
   }

   public Directories getDirectories() {
      return this.directories;
   }

   public synchronized void pause() {
      this.isActive = false;
   }

   public synchronized void resume() {
      this.isActive = true;
   }

   public void startup() {
      this.isActive = true;
   }

   public void shutdown() {
      this.isActive = false;
   }

   public abstract AbstractCompactionTask getNextBackgroundTask(int var1);

   public abstract Collection<AbstractCompactionTask> getMaximalTask(int var1, boolean var2);

   public abstract AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> var1, int var2);

   public AbstractCompactionTask getCompactionTask(LifecycleTransaction txn, int gcBefore, long maxSSTableBytes) {
      return new CompactionTask(this.cfs, txn, gcBefore);
   }

   public abstract int getEstimatedRemainingTasks();

   public abstract long getMaxSSTableBytes();

   public static Iterable<SSTableReader> filterSuspectSSTables(Iterable<SSTableReader> originalCandidates) {
      return Iterables.filter(originalCandidates, new Predicate<SSTableReader>() {
         public boolean apply(SSTableReader sstable) {
            return !sstable.isMarkedSuspect();
         }
      });
   }

   public AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> sstables, Range<Token> range) {
      return range == null?this.getScanners(sstables, (Collection)null):this.getScanners(sstables, (Collection)Collections.singleton(range));
   }

   public AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges) {
      ArrayList scanners = new ArrayList();

      try {
         Iterator var4 = sstables.iterator();

         while(var4.hasNext()) {
            SSTableReader sstable = (SSTableReader)var4.next();
            scanners.add(sstable.getScanner(ranges));
         }
      } catch (Throwable var6) {
         ISSTableScanner.closeAllAndPropagate(scanners, var6);
      }

      return new AbstractCompactionStrategy.ScannerList(scanners);
   }

   public boolean shouldDefragment() {
      return false;
   }

   public String getName() {
      return this.getClass().getSimpleName();
   }

   public synchronized void replaceSSTables(Collection<SSTableReader> removed, Collection<SSTableReader> added) {
      Iterator var3 = removed.iterator();

      SSTableReader add;
      while(var3.hasNext()) {
         add = (SSTableReader)var3.next();
         this.removeSSTable(add);
      }

      var3 = added.iterator();

      while(var3.hasNext()) {
         add = (SSTableReader)var3.next();
         this.addSSTable(add);
      }

   }

   public abstract void addSSTable(SSTableReader var1);

   public synchronized void addSSTables(Iterable<SSTableReader> added) {
      Iterator var2 = added.iterator();

      while(var2.hasNext()) {
         SSTableReader sstable = (SSTableReader)var2.next();
         this.addSSTable(sstable);
      }

   }

   public void addSSTableFromStreaming(SSTableReader added) {
      this.addSSTable(added);
   }

   public abstract void removeSSTable(SSTableReader var1);

   @VisibleForTesting
   protected abstract Set<SSTableReader> getSSTables();

   public AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> toCompact) {
      return this.getScanners(toCompact, (Collection)null);
   }

   protected boolean worthDroppingTombstones(SSTableReader sstable, int gcBefore) {
      if(!this.disableTombstoneCompactions && !CompactionController.NEVER_PURGE_TOMBSTONES) {
         if(System.currentTimeMillis() < sstable.getCreationTimeFor(Component.DATA) + this.tombstoneCompactionInterval * 1000L) {
            return false;
         } else {
            double droppableRatio = sstable.getEstimatedDroppableTombstoneRatio(gcBefore);
            if(droppableRatio <= (double)this.tombstoneThreshold) {
               return false;
            } else if(this.uncheckedTombstoneCompaction) {
               return true;
            } else {
               Collection<SSTableReader> overlaps = this.cfs.getOverlappingLiveSSTables(Collections.singleton(sstable));
               if(overlaps.isEmpty()) {
                  return true;
               } else if(CompactionController.getFullyExpiredSSTables(this.cfs, Collections.singleton(sstable), overlaps, gcBefore).size() > 0) {
                  return true;
               } else if(sstable.estimatedKeys() < 512L) {
                  return false;
               } else {
                  long keys = sstable.estimatedKeys();
                  Set<Range<Token>> ranges = new HashSet(overlaps.size());
                  Iterator var9 = overlaps.iterator();

                  while(var9.hasNext()) {
                     SSTableReader overlap = (SSTableReader)var9.next();
                     ranges.add(new Range(overlap.first.getToken(), overlap.last.getToken()));
                  }

                  long remainingKeys = keys - sstable.estimatedKeysForRanges(ranges);
                  long columns = sstable.getEstimatedColumnCount().mean() * remainingKeys;
                  double remainingColumnsRatio = (double)columns / (double)(sstable.getEstimatedColumnCount().count() * sstable.getEstimatedColumnCount().mean());
                  return remainingColumnsRatio * droppableRatio > (double)this.tombstoneThreshold;
               }
            }
         }
      } else {
         return false;
      }
   }

   public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException {
      String threshold = (String)options.get("tombstone_threshold");
      if(threshold != null) {
         try {
            float thresholdValue = Float.parseFloat(threshold);
            if(thresholdValue < 0.0F) {
               throw new ConfigurationException(String.format("%s must be greater than 0, but was %f", new Object[]{"tombstone_threshold", Float.valueOf(thresholdValue)}));
            }
         } catch (NumberFormatException var8) {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", new Object[]{threshold, "tombstone_threshold"}), var8);
         }
      }

      String interval = (String)options.get("tombstone_compaction_interval");
      if(interval != null) {
         try {
            long tombstoneCompactionInterval = Long.parseLong(interval);
            if(tombstoneCompactionInterval < 0L) {
               throw new ConfigurationException(String.format("%s must be greater than 0, but was %d", new Object[]{"tombstone_compaction_interval", Long.valueOf(tombstoneCompactionInterval)}));
            }
         } catch (NumberFormatException var7) {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", new Object[]{interval, "tombstone_compaction_interval"}), var7);
         }
      }

      String unchecked = (String)options.get("unchecked_tombstone_compaction");
      if(unchecked != null && !unchecked.equalsIgnoreCase("true") && !unchecked.equalsIgnoreCase("false")) {
         throw new ConfigurationException(String.format("'%s' should be either 'true' or 'false', not '%s'", new Object[]{"unchecked_tombstone_compaction", unchecked}));
      } else {
         String logAll = (String)options.get("log_all");
         if(logAll != null && !logAll.equalsIgnoreCase("true") && !logAll.equalsIgnoreCase("false")) {
            throw new ConfigurationException(String.format("'%s' should either be 'true' or 'false', not %s", new Object[]{"log_all", logAll}));
         } else {
            String compactionEnabled = (String)options.get("enabled");
            if(compactionEnabled != null && !compactionEnabled.equalsIgnoreCase("true") && !compactionEnabled.equalsIgnoreCase("false")) {
               throw new ConfigurationException(String.format("enabled should either be 'true' or 'false', not %s", new Object[]{compactionEnabled}));
            } else {
               Map<String, String> uncheckedOptions = new HashMap(options);
               uncheckedOptions.remove("tombstone_threshold");
               uncheckedOptions.remove("tombstone_compaction_interval");
               uncheckedOptions.remove("unchecked_tombstone_compaction");
               uncheckedOptions.remove("log_all");
               uncheckedOptions.remove("enabled");
               uncheckedOptions.remove("only_purge_repaired_tombstones");
               uncheckedOptions.remove(CompactionParams.Option.PROVIDE_OVERLAPPING_TOMBSTONES.toString());
               return uncheckedOptions;
            }
         }
      }
   }

   public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> sstablesToGroup) {
      int groupSize = 2;
      List<SSTableReader> sortedSSTablesToGroup = new ArrayList(sstablesToGroup);
      Collections.sort(sortedSSTablesToGroup, SSTableReader.sstableComparator);
      Collection<Collection<SSTableReader>> groupedSSTables = new ArrayList();
      Collection<SSTableReader> currGroup = new ArrayList(groupSize);
      Iterator var6 = sortedSSTablesToGroup.iterator();

      while(var6.hasNext()) {
         SSTableReader sstable = (SSTableReader)var6.next();
         currGroup.add(sstable);
         if(currGroup.size() == groupSize) {
            groupedSSTables.add(currGroup);
            currGroup = new ArrayList(groupSize);
         }
      }

      if(currGroup.size() != 0) {
         groupedSSTables.add(currGroup);
      }

      return groupedSSTables;
   }

   public CompactionLogger.Strategy strategyLogger() {
      return CompactionLogger.Strategy.none;
   }

   public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, MetadataCollector meta, SerializationHeader header, Collection<Index> indexes, LifecycleTransaction txn) {
      return SimpleSSTableMultiWriter.create(descriptor, keyCount, repairedAt, pendingRepair, this.cfs.metadata, meta, header, indexes, txn);
   }

   public boolean supportsEarlyOpen() {
      return true;
   }

   public static class ScannerList implements AutoCloseable {
      public final List<ISSTableScanner> scanners;

      public ScannerList(List<ISSTableScanner> scanners) {
         this.scanners = scanners;
      }

      public long getTotalBytesScanned() {
         long bytesScanned = 0L;

         ISSTableScanner scanner;
         for(Iterator var3 = this.scanners.iterator(); var3.hasNext(); bytesScanned += scanner.getBytesScanned()) {
            scanner = (ISSTableScanner)var3.next();
         }

         return bytesScanned;
      }

      public long getTotalCompressedSize() {
         long compressedSize = 0L;

         ISSTableScanner scanner;
         for(Iterator var3 = this.scanners.iterator(); var3.hasNext(); compressedSize += scanner.getCompressedLengthInBytes()) {
            scanner = (ISSTableScanner)var3.next();
         }

         return compressedSize;
      }

      public double getCompressionRatio() {
         double compressed = 0.0D;
         double uncompressed = 0.0D;

         ISSTableScanner scanner;
         for(Iterator var5 = this.scanners.iterator(); var5.hasNext(); uncompressed += (double)scanner.getLengthInBytes()) {
            scanner = (ISSTableScanner)var5.next();
            compressed += (double)scanner.getCompressedLengthInBytes();
         }

         return compressed != uncompressed && uncompressed != 0.0D?compressed / uncompressed:-1.0D;
      }

      public void close() {
         ISSTableScanner.closeAllAndPropagate(this.scanners, (Throwable)null);
      }
   }
}
