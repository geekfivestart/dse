package org.apache.cassandra.db.compaction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Sets.SetView;
import com.google.common.primitives.Ints;
import java.io.IOException;
import java.util.ArrayList;
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
import java.util.SortedSet;
import java.util.Map.Entry;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeveledManifest {
   private static final Logger logger = LoggerFactory.getLogger(LeveledManifest.class);
   private static final int MAX_COMPACTING_L0 = 32;
   private static final int NO_COMPACTION_LIMIT = 25;
   public static final int MAX_LEVEL_COUNT = (int)Math.log10(1.0E9D);
   private final ColumnFamilyStore cfs;
   @VisibleForTesting
   protected final List<SSTableReader>[] generations;
   private final PartitionPosition[] lastCompactedKeys;
   private final long maxSSTableSizeInBytes;
   private final SizeTieredCompactionStrategyOptions options;
   private final int[] compactionCounter;
   private final int levelFanoutSize;
   private static final Predicate<SSTableReader> suspectP = new Predicate<SSTableReader>() {
      public boolean apply(SSTableReader candidate) {
         return candidate.isMarkedSuspect();
      }
   };

   LeveledManifest(ColumnFamilyStore cfs, int maxSSTableSizeInMB, int fanoutSize, SizeTieredCompactionStrategyOptions options) {
      this.cfs = cfs;
      this.maxSSTableSizeInBytes = (long)maxSSTableSizeInMB * 1024L * 1024L;
      this.options = options;
      this.levelFanoutSize = fanoutSize;
      this.generations = new List[MAX_LEVEL_COUNT];
      this.lastCompactedKeys = new PartitionPosition[MAX_LEVEL_COUNT];

      for(int i = 0; i < this.generations.length; ++i) {
         this.generations[i] = new ArrayList();
         this.lastCompactedKeys[i] = cfs.getPartitioner().getMinimumToken().minKeyBound();
      }

      this.compactionCounter = new int[MAX_LEVEL_COUNT];
   }

   public static LeveledManifest create(ColumnFamilyStore cfs, int maxSSTableSize, int fanoutSize, List<SSTableReader> sstables) {
      return create(cfs, maxSSTableSize, fanoutSize, sstables, new SizeTieredCompactionStrategyOptions());
   }

   public static LeveledManifest create(ColumnFamilyStore cfs, int maxSSTableSize, int fanoutSize, Iterable<SSTableReader> sstables, SizeTieredCompactionStrategyOptions options) {
      LeveledManifest manifest = new LeveledManifest(cfs, maxSSTableSize, fanoutSize, options);
      Iterator var6 = sstables.iterator();

      while(var6.hasNext()) {
         SSTableReader ssTableReader = (SSTableReader)var6.next();
         manifest.add(ssTableReader, false);
      }

      for(int i = 1; i < manifest.getAllLevelSize().length; ++i) {
         manifest.repairOverlappingSSTables(i);
      }

      manifest.calculateLastCompactedKeys();
      return manifest;
   }

   public void calculateLastCompactedKeys() {
      for(int i = 0; i < this.generations.length - 1; ++i) {
         if(!this.generations[i + 1].isEmpty()) {
            SSTableReader sstableWithMaxModificationTime = null;
            long maxModificationTime = -9223372036854775808L;
            Iterator var5 = this.generations[i + 1].iterator();

            while(var5.hasNext()) {
               SSTableReader ssTableReader = (SSTableReader)var5.next();
               long modificationTime = ssTableReader.getCreationTimeFor(Component.DATA);
               if(modificationTime >= maxModificationTime) {
                  sstableWithMaxModificationTime = ssTableReader;
                  maxModificationTime = modificationTime;
               }
            }

            this.lastCompactedKeys[i] = sstableWithMaxModificationTime.last;
         }
      }

   }

   public synchronized void add(SSTableReader reader, boolean isStreaming) {
      int level = reader.getSSTableLevel();

      assert level < this.generations.length : "Invalid level " + level + " out of " + (this.generations.length - 1);

      this.logDistribution();
      int pickedLevel = level;
      if(isStreaming && DatabaseDescriptor.isPickLevelOnStreaming()) {
         pickedLevel = this.pickSSTableLevel(level, 0, reader);
      } else if(!this.canAddSSTable(level, reader)) {
         pickedLevel = 0;
      }

      if(pickedLevel == level) {
         logger.trace("Adding {} to L{}", reader, Integer.valueOf(level));
         this.generations[level].add(reader);
      } else {
         try {
            logger.debug("Moving sstable {} from L{} to L{} due to overlap/overflow in level", new Object[]{reader.descriptor, Integer.valueOf(level), Integer.valueOf(pickedLevel)});
            reader.descriptor.getMetadataSerializer().mutateLevel(reader.descriptor, pickedLevel);
            reader.reloadSSTableMetadata();
         } catch (IOException var6) {
            logger.error("Could not change sstable level - it will end up at L0, we will find it at restart.", var6);
         }

         if(!this.contains(reader)) {
            this.generations[pickedLevel].add(reader);
         } else {
            logger.warn("SSTable {} is already present on leveled manifest and should not be re-added.", reader, new RuntimeException());
         }
      }

   }

   private boolean contains(SSTableReader reader) {
      for(int i = 0; i < this.generations.length; ++i) {
         if(this.generations[i].contains(reader)) {
            return true;
         }
      }

      return false;
   }

   public synchronized void replace(Collection<SSTableReader> removed, Collection<SSTableReader> added) {
      assert !removed.isEmpty();

      this.logDistribution();
      if(logger.isTraceEnabled()) {
         logger.trace("Replacing [{}]", this.toString(removed));
      }

      int minLevel = 2147483647;

      Iterator var4;
      SSTableReader ssTableReader;
      int thisLevel;
      for(var4 = removed.iterator(); var4.hasNext(); minLevel = Math.min(minLevel, thisLevel)) {
         ssTableReader = (SSTableReader)var4.next();
         thisLevel = this.remove(ssTableReader);
      }

      if(!added.isEmpty()) {
         if(logger.isTraceEnabled()) {
            logger.trace("Adding [{}]", this.toString(added));
         }

         var4 = added.iterator();

         while(var4.hasNext()) {
            ssTableReader = (SSTableReader)var4.next();
            this.add(ssTableReader, false);
         }

         this.lastCompactedKeys[minLevel] = ((SSTableReader)SSTableReader.sstableOrdering.max(added)).last;
      }
   }

   public synchronized void repairOverlappingSSTables(int level) {
      SSTableReader previous = null;
      Collections.sort(this.generations[level], SSTableReader.sstableComparator);
      List<SSTableReader> outOfOrderSSTables = new ArrayList();
      Iterator var4 = this.generations[level].iterator();

      while(true) {
         SSTableReader sstable;
         while(var4.hasNext()) {
            sstable = (SSTableReader)var4.next();
            if(previous != null && sstable.first.compareTo((PartitionPosition)previous.last) <= 0) {
               logger.warn("At level {}, {} [{}, {}] overlaps {} [{}, {}].  This could be caused by a bug in Cassandra 1.1.0 .. 1.1.3 or due to the fact that you have dropped sstables from another node into the data directory. Sending back to L0.  If you didn't drop in sstables, and have not yet run scrub, you should do so since you may also have rows out-of-order within an sstable", new Object[]{Integer.valueOf(level), previous, previous.first, previous.last, sstable, sstable.first, sstable.last});
               outOfOrderSSTables.add(sstable);
            } else {
               previous = sstable;
            }
         }

         if(!outOfOrderSSTables.isEmpty()) {
            var4 = outOfOrderSSTables.iterator();

            while(var4.hasNext()) {
               sstable = (SSTableReader)var4.next();
               this.sendBackToL0(sstable);
            }
         }

         return;
      }
   }

   private boolean canAddSSTable(int level, SSTableReader sstable) {
      if(level == 0) {
         return true;
      } else {
         List<SSTableReader> copyLevel = new ArrayList(this.generations[level]);
         copyLevel.add(sstable);
         Collections.sort(copyLevel, SSTableReader.sstableComparator);
         SSTableReader previous = null;

         SSTableReader current;
         for(Iterator var5 = copyLevel.iterator(); var5.hasNext(); previous = current) {
            current = (SSTableReader)var5.next();
            if(previous != null && current.first.compareTo((PartitionPosition)previous.last) <= 0) {
               return false;
            }
         }

         return true;
      }
   }

   private int pickSSTableLevel(int suggestedLevel, int firstNonOverlappingLevel, SSTableReader sstable) {
      if(suggestedLevel == 0) {
         return 0;
      } else {
         int pickedLevel = suggestedLevel;
         boolean overlapping = !this.canAddSSTable(suggestedLevel, sstable);
         if(!overlapping && firstNonOverlappingLevel == 0) {
            firstNonOverlappingLevel = suggestedLevel;
         }

         if(overlapping || this.levelScoreWithSStable(suggestedLevel, sstable) > 1.001D) {
            if(suggestedLevel + 1 < this.generations.length) {
               pickedLevel = this.pickSSTableLevel(suggestedLevel + 1, firstNonOverlappingLevel, sstable);
               if(pickedLevel == 0) {
                  pickedLevel = firstNonOverlappingLevel;
               }
            } else {
               pickedLevel = 0;
            }
         }

         if(logger.isTraceEnabled()) {
            logger.trace("suggested = {}, overlapped = {}, score = {} picked = {}", new Object[]{Integer.valueOf(suggestedLevel), Boolean.valueOf(overlapping), Double.valueOf(this.levelScoreWithSStable(suggestedLevel, sstable)), Integer.valueOf(pickedLevel)});
         }

         return pickedLevel;
      }
   }

   private double levelScoreWithSStable(int level, SSTableReader sstable) {
      List<SSTableReader> copyLevel = new ArrayList(this.generations[level]);
      copyLevel.add(sstable);
      return (double)SSTableReader.getTotalBytes(copyLevel) / (double)this.maxBytesForLevel(level, this.maxSSTableSizeInBytes);
   }

   private synchronized void sendBackToL0(SSTableReader sstable) {
      this.remove(sstable);

      try {
         sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 0);
         sstable.reloadSSTableMetadata();
         this.add(sstable, false);
      } catch (IOException var3) {
         throw new RuntimeException("Could not reload sstable meta data", var3);
      }
   }

   private String toString(Collection<SSTableReader> sstables) {
      StringBuilder builder = new StringBuilder();
      Iterator var3 = sstables.iterator();

      while(var3.hasNext()) {
         SSTableReader sstable = (SSTableReader)var3.next();
         builder.append(sstable.descriptor.cfname).append('-').append(sstable.descriptor.generation).append("(L").append(sstable.getSSTableLevel()).append("), ");
      }

      return builder.toString();
   }

   public long maxBytesForLevel(int level, long maxSSTableSizeInBytes) {
      return maxBytesForLevel(level, this.levelFanoutSize, maxSSTableSizeInBytes);
   }

   public static long maxBytesForLevel(int level, int levelFanoutSize, long maxSSTableSizeInBytes) {
      if(level == 0) {
         return 4L * maxSSTableSizeInBytes;
      } else {
         double bytes = Math.pow((double)levelFanoutSize, (double)level) * (double)maxSSTableSizeInBytes;
         if(bytes > 9.223372036854776E18D) {
            throw new RuntimeException("At most 9223372036854775807 bytes may be in a compaction level; your maxSSTableSize must be absurdly high to compute " + bytes);
         } else {
            return (long)bytes;
         }
      }
   }

   public synchronized LeveledManifest.CompactionCandidate getCompactionCandidates() {
      if(StorageService.instance.isBootstrapMode()) {
         List<SSTableReader> mostInteresting = this.getSSTablesForSTCS(this.getLevel(0));
         if(!mostInteresting.isEmpty()) {
            logger.info("Bootstrapping - doing STCS in L0");
            return new LeveledManifest.CompactionCandidate(mostInteresting, 0, 9223372036854775807L);
         } else {
            return null;
         }
      } else {
         LeveledManifest.CompactionCandidate l0Compaction = this.getSTCSInL0CompactionCandidate();

         for(int i = this.generations.length - 1; i > 0; --i) {
            List<SSTableReader> sstables = this.getLevel(i);
            if(!sstables.isEmpty()) {
               Set<SSTableReader> sstablesInLevel = Sets.newHashSet(sstables);
               Set<SSTableReader> remaining = Sets.difference(sstablesInLevel, this.cfs.getTracker().getCompacting());
               double score = (double)SSTableReader.getTotalBytes(remaining) / (double)this.maxBytesForLevel(i, this.maxSSTableSizeInBytes);
               logger.trace("Compaction score for level {} is {}", Integer.valueOf(i), Double.valueOf(score));
               if(score > 1.001D) {
                  if(l0Compaction != null) {
                     return l0Compaction;
                  }

                  Collection<SSTableReader> candidates = this.getCandidatesFor(i);
                  if(!candidates.isEmpty()) {
                     int nextLevel = this.getNextLevel(candidates);
                     candidates = this.getOverlappingStarvedSSTables(nextLevel, candidates);
                     if(logger.isTraceEnabled()) {
                        logger.trace("Compaction candidates for L{} are {}", Integer.valueOf(i), this.toString(candidates));
                     }

                     return new LeveledManifest.CompactionCandidate(candidates, nextLevel, this.cfs.getCompactionStrategyManager().getMaxSSTableBytes());
                  }

                  logger.trace("No compaction candidates for L{}", Integer.valueOf(i));
               }
            }
         }

         if(this.getLevel(0).isEmpty()) {
            return null;
         } else {
            Collection<SSTableReader> candidates = this.getCandidatesFor(0);
            if(candidates.isEmpty()) {
               return l0Compaction;
            } else {
               return new LeveledManifest.CompactionCandidate(candidates, this.getNextLevel(candidates), this.maxSSTableSizeInBytes);
            }
         }
      }
   }

   private LeveledManifest.CompactionCandidate getSTCSInL0CompactionCandidate() {
      if(!DatabaseDescriptor.getDisableSTCSInL0() && this.getLevel(0).size() > 32) {
         List<SSTableReader> mostInteresting = this.getSSTablesForSTCS(this.getLevel(0));
         if(!mostInteresting.isEmpty()) {
            logger.debug("L0 is too far behind, performing size-tiering there first");
            return new LeveledManifest.CompactionCandidate(mostInteresting, 0, 9223372036854775807L);
         }
      }

      return null;
   }

   private List<SSTableReader> getSSTablesForSTCS(Collection<SSTableReader> sstables) {
      Iterable<SSTableReader> candidates = this.cfs.getTracker().getUncompacting(sstables);
      List<Pair<SSTableReader, Long>> pairs = SizeTieredCompactionStrategy.createSSTableAndLengthPairs(AbstractCompactionStrategy.filterSuspectSSTables(candidates));
      List<List<SSTableReader>> buckets = SizeTieredCompactionStrategy.getBuckets(pairs, this.options.bucketHigh, this.options.bucketLow, this.options.minSSTableSize);
      return SizeTieredCompactionStrategy.mostInterestingBucket(buckets, this.cfs.getMinimumCompactionThreshold(), this.cfs.getMaximumCompactionThreshold());
   }

   private Collection<SSTableReader> getOverlappingStarvedSSTables(int targetLevel, Collection<SSTableReader> candidates) {
      Set<SSTableReader> withStarvedCandidate = new HashSet(candidates);

      int i;
      for(i = this.generations.length - 1; i > 0; --i) {
         ++this.compactionCounter[i];
      }

      this.compactionCounter[targetLevel] = 0;
      if(logger.isTraceEnabled()) {
         for(i = 0; i < this.compactionCounter.length; ++i) {
            logger.trace("CompactionCounter: {}: {}", Integer.valueOf(i), Integer.valueOf(this.compactionCounter[i]));
         }
      }

      for(i = this.generations.length - 1; i > 0; --i) {
         if(this.getLevelSize(i) > 0) {
            if(this.compactionCounter[i] > 25) {
               PartitionPosition max = null;
               PartitionPosition min = null;
               Iterator var7 = candidates.iterator();

               while(var7.hasNext()) {
                  SSTableReader candidate = (SSTableReader)var7.next();
                  if(min == null || candidate.first.compareTo((PartitionPosition)min) < 0) {
                     min = candidate.first;
                  }

                  if(max == null || candidate.last.compareTo((PartitionPosition)max) > 0) {
                     max = candidate.last;
                  }
               }

               if(min == null || max == null || min.equals(max)) {
                  return candidates;
               }

               Set<SSTableReader> compacting = this.cfs.getTracker().getCompacting();
               Range<PartitionPosition> boundaries = new Range(min, max);
               Iterator var9 = this.getLevel(i).iterator();

               SSTableReader sstable;
               Range r;
               do {
                  if(!var9.hasNext()) {
                     return candidates;
                  }

                  sstable = (SSTableReader)var9.next();
                  r = new Range(sstable.first, sstable.last);
               } while(!boundaries.contains((AbstractBounds)r) || compacting.contains(sstable));

               logger.info("Adding high-level (L{}) {} to candidates", Integer.valueOf(sstable.getSSTableLevel()), sstable);
               withStarvedCandidate.add(sstable);
               return withStarvedCandidate;
            }

            return candidates;
         }
      }

      return candidates;
   }

   public synchronized int getLevelSize(int i) {
      if(i >= this.generations.length) {
         throw new ArrayIndexOutOfBoundsException("Maximum valid generation is " + (this.generations.length - 1));
      } else {
         return this.getLevel(i).size();
      }
   }

   public synchronized int[] getAllLevelSize() {
      int[] counts = new int[this.generations.length];

      for(int i = 0; i < counts.length; ++i) {
         counts[i] = this.getLevel(i).size();
      }

      return counts;
   }

   private void logDistribution() {
      if(logger.isTraceEnabled()) {
         for(int i = 0; i < this.generations.length; ++i) {
            if(!this.getLevel(i).isEmpty()) {
               logger.trace("L{} contains {} SSTables ({}) in {}", new Object[]{Integer.valueOf(i), Integer.valueOf(this.getLevel(i).size()), FBUtilities.prettyPrintMemory(SSTableReader.getTotalBytes(this.getLevel(i))), this});
            }
         }
      }

   }

   @VisibleForTesting
   public synchronized int remove(SSTableReader reader) {
      int level = reader.getSSTableLevel();

      assert level >= 0 : reader + " not present in manifest: " + level;

      this.generations[level].remove(reader);
      return level;
   }

   public synchronized Set<SSTableReader> getSSTables() {
      Builder<SSTableReader> builder = ImmutableSet.builder();
      List[] var2 = this.generations;
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         List<SSTableReader> sstables = var2[var4];
         builder.addAll(sstables);
      }

      return builder.build();
   }

   private static Set<SSTableReader> overlapping(Collection<SSTableReader> candidates, Iterable<SSTableReader> others) {
      assert !candidates.isEmpty();

      Iterator<SSTableReader> iter = candidates.iterator();
      SSTableReader sstable = (SSTableReader)iter.next();
      Token first = sstable.first.getToken();

      Token last;
      for(last = sstable.last.getToken(); iter.hasNext(); last = last.compareTo(sstable.last.getToken()) >= 0?last:sstable.last.getToken()) {
         sstable = (SSTableReader)iter.next();
         first = first.compareTo(sstable.first.getToken()) <= 0?first:sstable.first.getToken();
      }

      return overlapping(first, last, others);
   }

   private static Set<SSTableReader> overlappingWithBounds(SSTableReader sstable, Map<SSTableReader, Bounds<Token>> others) {
      return overlappingWithBounds(sstable.first.getToken(), sstable.last.getToken(), others);
   }

   @VisibleForTesting
   static Set<SSTableReader> overlapping(Token start, Token end, Iterable<SSTableReader> sstables) {
      return overlappingWithBounds(start, end, genBounds(sstables));
   }

   private static Set<SSTableReader> overlappingWithBounds(Token start, Token end, Map<SSTableReader, Bounds<Token>> sstables) {
      assert start.compareTo(end) <= 0;

      Set<SSTableReader> overlapped = new HashSet();
      Bounds<Token> promotedBounds = new Bounds(start, end);
      Iterator var5 = sstables.entrySet().iterator();

      while(var5.hasNext()) {
         Entry<SSTableReader, Bounds<Token>> pair = (Entry)var5.next();
         if(((Bounds)pair.getValue()).intersects(promotedBounds)) {
            overlapped.add(pair.getKey());
         }
      }

      return overlapped;
   }

   private static Map<SSTableReader, Bounds<Token>> genBounds(Iterable<SSTableReader> ssTableReaders) {
      Map<SSTableReader, Bounds<Token>> boundsMap = new HashMap();
      Iterator var2 = ssTableReaders.iterator();

      while(var2.hasNext()) {
         SSTableReader sstable = (SSTableReader)var2.next();
         boundsMap.put(sstable, new Bounds(sstable.first.getToken(), sstable.last.getToken()));
      }

      return boundsMap;
   }

   private Collection<SSTableReader> getCandidatesFor(int level) {
      assert !this.getLevel(level).isEmpty();

      logger.trace("Choosing candidates for L{}", Integer.valueOf(level));
      Set<SSTableReader> compacting = this.cfs.getTracker().getCompacting();
      if(level == 0) {
         Set<SSTableReader> compactingL0 = this.getCompacting(0);
         PartitionPosition lastCompactingKey = null;
         PartitionPosition firstCompactingKey = null;
         Iterator var18 = compactingL0.iterator();

         while(true) {
            SSTableReader candidate;
            do {
               if(!var18.hasNext()) {
                  Set<SSTableReader> candidates = new HashSet();
                  Map<SSTableReader, Bounds<Token>> remaining = genBounds(Iterables.filter(this.getLevel(0), Predicates.not(suspectP)));
                  Iterator var8 = this.ageSortedSSTables(remaining.keySet()).iterator();

                  while(var8.hasNext()) {
                     SSTableReader sstable = (SSTableReader)var8.next();
                     if(!((Set)candidates).contains(sstable)) {
                        SetView<SSTableReader> overlappedL0 = Sets.union(Collections.singleton(sstable), overlappingWithBounds(sstable, remaining));
                        if(Sets.intersection(overlappedL0, compactingL0).isEmpty()) {
                           SSTableReader newCandidate;
                           for(Iterator var11 = overlappedL0.iterator(); var11.hasNext(); remaining.remove(newCandidate)) {
                              newCandidate = (SSTableReader)var11.next();
                              if(firstCompactingKey == null || lastCompactingKey == null || overlapping(firstCompactingKey.getToken(), lastCompactingKey.getToken(), Arrays.asList(new SSTableReader[]{newCandidate})).size() == 0) {
                                 ((Set)candidates).add(newCandidate);
                              }
                           }

                           if(((Set)candidates).size() > 32) {
                              candidates = new HashSet(this.ageSortedSSTables((Collection)candidates).subList(0, 32));
                              break;
                           }
                        }
                     }
                  }

                  if(SSTableReader.getTotalBytes((Iterable)candidates) > this.maxSSTableSizeInBytes) {
                     Set<SSTableReader> l1overlapping = overlapping((Collection)candidates, this.getLevel(1));
                     if(Sets.intersection(l1overlapping, compacting).size() > 0) {
                        return Collections.emptyList();
                     }

                     if(!overlapping((Collection)candidates, compactingL0).isEmpty()) {
                        return Collections.emptyList();
                     }

                     candidates = Sets.union((Set)candidates, l1overlapping);
                  }

                  if(((Set)candidates).size() < 2) {
                     return Collections.emptyList();
                  }

                  return (Collection)candidates;
               }

               candidate = (SSTableReader)var18.next();
               if(firstCompactingKey == null || candidate.first.compareTo((PartitionPosition)firstCompactingKey) < 0) {
                  firstCompactingKey = candidate.first;
               }
            } while(lastCompactingKey != null && candidate.last.compareTo((PartitionPosition)lastCompactingKey) <= 0);

            lastCompactingKey = candidate.last;
         }
      } else {
         Collections.sort(this.getLevel(level), SSTableReader.sstableComparator);
         int start = 0;

         for(int i = 0; i < this.getLevel(level).size(); ++i) {
            SSTableReader sstable = (SSTableReader)this.getLevel(level).get(i);
            if(sstable.first.compareTo(this.lastCompactedKeys[level]) > 0) {
               start = i;
               break;
            }
         }

         Map<SSTableReader, Bounds<Token>> sstablesNextLevel = genBounds(this.getLevel(level + 1));

         for(int i = 0; i < this.getLevel(level).size(); ++i) {
            SSTableReader sstable = (SSTableReader)this.getLevel(level).get((start + i) % this.getLevel(level).size());
            Set<SSTableReader> candidates = Sets.union(Collections.singleton(sstable), overlappingWithBounds(sstable, sstablesNextLevel));
            if(!Iterables.any(candidates, suspectP) && Sets.intersection(candidates, compacting).isEmpty()) {
               return candidates;
            }
         }

         return Collections.emptyList();
      }
   }

   private Set<SSTableReader> getCompacting(int level) {
      Set<SSTableReader> sstables = new HashSet();
      Set<SSTableReader> levelSSTables = new HashSet(this.getLevel(level));
      Iterator var4 = this.cfs.getTracker().getCompacting().iterator();

      while(var4.hasNext()) {
         SSTableReader sstable = (SSTableReader)var4.next();
         if(levelSSTables.contains(sstable)) {
            sstables.add(sstable);
         }
      }

      return sstables;
   }

   private List<SSTableReader> ageSortedSSTables(Collection<SSTableReader> candidates) {
      List<SSTableReader> ageSortedCandidates = new ArrayList(candidates);
      Collections.sort(ageSortedCandidates, SSTableReader.maxTimestampComparator);
      return ageSortedCandidates;
   }

   public synchronized Set<SSTableReader>[] getSStablesPerLevelSnapshot() {
      Set<SSTableReader>[] sstablesPerLevel = new Set[this.generations.length];

      for(int i = 0; i < this.generations.length; ++i) {
         sstablesPerLevel[i] = new HashSet(this.generations[i]);
      }

      return sstablesPerLevel;
   }

   public String toString() {
      return "Manifest@" + this.hashCode();
   }

   public int getLevelCount() {
      for(int i = this.generations.length - 1; i >= 0; --i) {
         if(this.getLevel(i).size() > 0) {
            return i;
         }
      }

      return 0;
   }

   public synchronized SortedSet<SSTableReader> getLevelSorted(int level, Comparator<SSTableReader> comparator) {
      return ImmutableSortedSet.copyOf(comparator, this.getLevel(level));
   }

   public List<SSTableReader> getLevel(int i) {
      return this.generations[i];
   }

   public synchronized int getEstimatedTasks() {
      long tasks = 0L;
      long[] estimated = new long[this.generations.length];

      int l0compactions;
      for(l0compactions = this.generations.length - 1; l0compactions >= 0; --l0compactions) {
         List<SSTableReader> sstables = this.getLevel(l0compactions);
         estimated[l0compactions] = (long)Math.ceil((double)Math.max(0L, SSTableReader.getTotalBytes(sstables) - (long)((double)this.maxBytesForLevel(l0compactions, this.maxSSTableSizeInBytes) * 1.001D)) / (double)this.maxSSTableSizeInBytes);
         tasks += estimated[l0compactions];
      }

      if(!DatabaseDescriptor.getDisableSTCSInL0() && this.getLevel(0).size() > 32) {
         l0compactions = this.getLevel(0).size() / 32;
         tasks += (long)l0compactions;
         estimated[0] += (long)l0compactions;
      }

      logger.trace("Estimating {} compactions to do for {}.{}", new Object[]{Arrays.toString(estimated), this.cfs.keyspace.getName(), this.cfs.name});
      return Ints.checkedCast(tasks);
   }

   public int getNextLevel(Collection<SSTableReader> sstables) {
      int maximumLevel = -2147483648;
      int minimumLevel = 2147483647;

      SSTableReader sstable;
      for(Iterator var4 = sstables.iterator(); var4.hasNext(); minimumLevel = Math.min(sstable.getSSTableLevel(), minimumLevel)) {
         sstable = (SSTableReader)var4.next();
         maximumLevel = Math.max(sstable.getSSTableLevel(), maximumLevel);
      }

      int newLevel;
      if(minimumLevel == 0 && minimumLevel == maximumLevel && SSTableReader.getTotalBytes(sstables) < this.maxSSTableSizeInBytes) {
         newLevel = 0;
      } else {
         newLevel = minimumLevel == maximumLevel?maximumLevel + 1:maximumLevel;

         assert newLevel > 0;
      }

      return newLevel;
   }

   public Iterable<SSTableReader> getAllSSTables() {
      Set<SSTableReader> sstables = new HashSet();
      List[] var2 = this.generations;
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         List<SSTableReader> generation = var2[var4];
         sstables.addAll(generation);
      }

      return sstables;
   }

   public static class CompactionCandidate {
      public final Collection<SSTableReader> sstables;
      public final int level;
      public final long maxSSTableBytes;

      public CompactionCandidate(Collection<SSTableReader> sstables, int level, long maxSSTableBytes) {
         this.sstables = sstables;
         this.level = level;
         this.maxSSTableBytes = maxSSTableBytes;
      }
   }
}
