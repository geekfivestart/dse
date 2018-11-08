package org.apache.cassandra.dht;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangeStreamer {
   private static final Logger logger = LoggerFactory.getLogger(RangeStreamer.class);
   private final Collection<Token> tokens;
   private final TokenMetadata metadata;
   private final InetAddress address;
   private final String description;
   private final Multimap<String, Entry<InetAddress, Collection<Range<Token>>>> toFetch = HashMultimap.create();
   private final RangeStreamer.ISourceFilter sourceFilter;
   private final StreamPlan streamPlan;
   private final boolean useStrictConsistency;
   private final IEndpointSnitch snitch;
   private final StreamStateStore stateStore;
   private StreamResultFuture streamFuture;

   public RangeStreamer(TokenMetadata metadata, Collection<Token> tokens, InetAddress address, StreamOperation streamOperation, boolean useStrictConsistency, IEndpointSnitch snitch, StreamStateStore stateStore, boolean connectSequentially, int connectionsPerHost, RangeStreamer.ISourceFilter sourceFilter) {
      this.metadata = metadata;
      this.tokens = tokens;
      this.address = address;
      this.description = streamOperation.getDescription();
      this.streamPlan = new StreamPlan(streamOperation, connectionsPerHost, true, connectSequentially, (UUID)null, PreviewKind.NONE);
      this.useStrictConsistency = useStrictConsistency;
      this.snitch = snitch;
      this.stateStore = stateStore;
      this.streamPlan.listeners(this.stateStore, new StreamEventHandler[0]);
      this.sourceFilter = sourceFilter;
   }

   public void addRanges(String keyspaceName, Collection<Range<Token>> ranges) {
      if(Keyspace.open(keyspaceName).getReplicationStrategy() instanceof LocalStrategy) {
         logger.info("Not adding ranges for Local Strategy keyspace={}", keyspaceName);
      } else {
         boolean useStrictSource = this.useStrictSourcesForRanges(keyspaceName);
         Multimap<Range<Token>, InetAddress> rangesForKeyspace = useStrictSource?this.getAllRangesWithStrictSourcesFor(keyspaceName, ranges):this.getAllRangesWithSourcesFor(keyspaceName, ranges);
         logger.info("Adding keyspace '{}'{} for ranges {}", new Object[]{keyspaceName, useStrictSource?" with strict sources":"", rangesForKeyspace.keySet()});
         Iterator var5 = rangesForKeyspace.entries().iterator();

         while(var5.hasNext()) {
            Entry<Range<Token>, InetAddress> entry = (Entry)var5.next();
            logger.info("{}: range {} exists on {} for keyspace {}", new Object[]{this.description, entry.getKey(), entry.getValue(), keyspaceName});
         }

         AbstractReplicationStrategy strat = Keyspace.open(keyspaceName).getReplicationStrategy();
         Multimap<InetAddress, Range<Token>> rangeFetchMap = !useStrictSource && strat != null && strat.getReplicationFactor() != 1?getOptimizedRangeFetchMap(rangesForKeyspace, this.sourceFilter, keyspaceName, this.useStrictConsistency):getRangeFetchMap(rangesForKeyspace, this.sourceFilter, keyspaceName, this.useStrictConsistency);

         Entry entry;
         for(Iterator var7 = rangeFetchMap.asMap().entrySet().iterator(); var7.hasNext(); this.toFetch.put(keyspaceName, entry)) {
            entry = (Entry)var7.next();
            if(logger.isTraceEnabled()) {
               Iterator var9 = ((Collection)entry.getValue()).iterator();

               while(var9.hasNext()) {
                  Range<Token> r = (Range)var9.next();
                  logger.trace("{}: range {} from source {} for keyspace {}", new Object[]{this.description, r, entry.getKey(), keyspaceName});
               }
            }
         }

      }
   }

   private boolean useStrictSourcesForRanges(String keyspaceName) {
      AbstractReplicationStrategy strat = Keyspace.open(keyspaceName).getReplicationStrategy();
      return this.useStrictConsistency && this.tokens != null && this.metadata.getSizeOfAllEndpoints() != strat.getReplicationFactor();
   }

   private Multimap<Range<Token>, InetAddress> getAllRangesWithSourcesFor(String keyspaceName, Collection<Range<Token>> desiredRanges) {
      AbstractReplicationStrategy strat = Keyspace.open(keyspaceName).getReplicationStrategy();
      Multimap<Range<Token>, InetAddress> rangeAddresses = strat.getRangeAddresses(this.metadata.cloneOnlyTokenMap());
      Multimap<Range<Token>, InetAddress> rangeSources = ArrayListMultimap.create();
      Iterator var6 = desiredRanges.iterator();

      Range desiredRange;
      do {
         if(!var6.hasNext()) {
            return rangeSources;
         }

         desiredRange = (Range)var6.next();
         Iterator var8 = rangeAddresses.keySet().iterator();

         while(var8.hasNext()) {
            Range<Token> range = (Range)var8.next();
            if(range.contains((AbstractBounds)desiredRange)) {
               List<InetAddress> preferred = this.snitch.getSortedListByProximity(this.address, rangeAddresses.get(range));
               rangeSources.putAll(desiredRange, preferred);
               break;
            }
         }
      } while(rangeSources.keySet().contains(desiredRange));

      throw new IllegalStateException("No sources found for " + desiredRange);
   }

   private Multimap<Range<Token>, InetAddress> getAllRangesWithStrictSourcesFor(String keyspace, Collection<Range<Token>> desiredRanges) {
      assert this.tokens != null;

      AbstractReplicationStrategy strat = Keyspace.open(keyspace).getReplicationStrategy();
      TokenMetadata metadataClone = this.metadata.cloneOnlyTokenMap();
      Multimap<Range<Token>, InetAddress> addressRanges = strat.getRangeAddresses(metadataClone);
      metadataClone.updateNormalTokens(this.tokens, this.address);
      Multimap<Range<Token>, InetAddress> pendingRangeAddresses = strat.getRangeAddresses(metadataClone);
      Multimap<Range<Token>, InetAddress> rangeSources = ArrayListMultimap.create();
      Iterator var8 = desiredRanges.iterator();

      while(var8.hasNext()) {
         Range<Token> desiredRange = (Range)var8.next();
         Iterator var10 = addressRanges.asMap().entrySet().iterator();

         while(var10.hasNext()) {
            Entry<Range<Token>, Collection<InetAddress>> preEntry = (Entry)var10.next();
            if(((Range)preEntry.getKey()).contains((AbstractBounds)desiredRange)) {
               Set<InetAddress> oldEndpoints = Sets.newHashSet((Iterable)preEntry.getValue());
               Set<InetAddress> newEndpoints = Sets.newHashSet(pendingRangeAddresses.get(desiredRange));
               if(oldEndpoints.size() == strat.getReplicationFactor()) {
                  oldEndpoints.removeAll(newEndpoints);

                  assert oldEndpoints.size() == 1 : "Expected 1 endpoint but found " + oldEndpoints.size();
               }

               rangeSources.put(desiredRange, oldEndpoints.iterator().next());
            }
         }

         Collection<InetAddress> addressList = rangeSources.get(desiredRange);
         if(addressList != null && !addressList.isEmpty()) {
            if(addressList.size() > 1) {
               throw new IllegalStateException("Multiple endpoints found for " + desiredRange);
            }

            InetAddress sourceIp = (InetAddress)addressList.iterator().next();
            EndpointState sourceState = Gossiper.instance.getEndpointStateForEndpoint(sourceIp);
            if(!Gossiper.instance.isEnabled() || sourceState != null && sourceState.isAlive()) {
               continue;
            }

            throw new RuntimeException("A node required to move the data consistently is down (" + sourceIp + "). If you wish to move the data from a potentially inconsistent replica, restart the node with -Dcassandra.consistent.rangemovement=false");
         }

         throw new IllegalStateException("No sources found for " + desiredRange);
      }

      return rangeSources;
   }

   private static Multimap<InetAddress, Range<Token>> getRangeFetchMap(Multimap<Range<Token>, InetAddress> rangesWithSources, RangeStreamer.ISourceFilter filter, String keyspace, boolean useStrictConsistency) {
      Multimap<InetAddress, Range<Token>> rangeFetchMapMap = HashMultimap.create();
      Iterator var5 = rangesWithSources.keySet().iterator();

      while(var5.hasNext()) {
         Range<Token> range = (Range)var5.next();
         boolean foundSource = false;
         Iterator var8 = rangesWithSources.get(range).iterator();

         while(var8.hasNext()) {
            InetAddress address = (InetAddress)var8.next();
            if(filter.shouldInclude(address)) {
               if(!address.equals(FBUtilities.getBroadcastAddress())) {
                  logger.info("Including {} for streaming range {} in keyspace {}", new Object[]{address, range, keyspace});
                  rangeFetchMapMap.put(address, range);
                  foundSource = true;
                  break;
               }

               foundSource = true;
            }
         }

         if(!foundSource) {
            handleSourceNotFound(keyspace, useStrictConsistency, range);
         }
      }

      return rangeFetchMapMap;
   }

   static void handleSourceNotFound(String keyspace, boolean useStrictConsistency, Range<Token> range) {
      AbstractReplicationStrategy strat = Keyspace.isInitialized()?Keyspace.open(keyspace).getReplicationStrategy():null;
      if(strat != null && strat.getReplicationFactor() == 1) {
         if(useStrictConsistency) {
            throw new IllegalStateException("Unable to find sufficient sources for streaming range " + range + " in keyspace " + keyspace + " with RF=1. Ensure this keyspace contains replicas in the source datacenter.");
         } else {
            logger.warn("Unable to find sufficient sources for streaming range {} in keyspace {} with RF=1. Keyspace might be missing data.", range, keyspace);
         }
      } else {
         throw new IllegalStateException("Unable to find sufficient sources for streaming range " + range + " in keyspace " + keyspace);
      }
   }

   private static Multimap<InetAddress, Range<Token>> getOptimizedRangeFetchMap(Multimap<Range<Token>, InetAddress> rangesWithSources, RangeStreamer.ISourceFilter filter, String keyspace, boolean useStrictConsistency) {
      RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources, filter, keyspace);
      return calculator.getRangeFetchMap(useStrictConsistency);
   }

   public static Multimap<InetAddress, Range<Token>> getWorkMapForMove(Multimap<Range<Token>, InetAddress> rangesWithSourceTarget, String keyspace, IFailureDetector fd, boolean useStrictConsistency) {
      return getRangeFetchMap(rangesWithSourceTarget, SourceFilters.failureDetectorFilter(fd), keyspace, useStrictConsistency);
   }

   @VisibleForTesting
   Multimap<String, Entry<InetAddress, Collection<Range<Token>>>> toFetch() {
      return this.toFetch;
   }

   public StreamResultFuture fetchAsync() {
      String keyspace;
      InetAddress source;
      InetAddress preferred;
      Collection ranges;
      for(Iterator var1 = this.toFetch.entries().iterator(); var1.hasNext(); this.streamPlan.requestRanges(source, preferred, keyspace, ranges)) {
         Entry<String, Entry<InetAddress, Collection<Range<Token>>>> entry = (Entry)var1.next();
         keyspace = (String)entry.getKey();
         source = (InetAddress)((Entry)entry.getValue()).getKey();
         preferred = SystemKeyspace.getPreferredIP(source);
         ranges = (Collection)((Entry)entry.getValue()).getValue();
         IPartitioner partitioner = StorageService.instance.getTokenMetadata().partitioner;
         Set<Range<Token>> availableRanges = (Set)TPCUtils.blockingGet(this.stateStore.getAvailableRanges(keyspace, partitioner));
         if(ranges.removeAll(availableRanges)) {
            logger.info("Some ranges of {} are already available. Skipping streaming those ranges.", availableRanges);
         }

         if(logger.isTraceEnabled()) {
            logger.trace("{}ing from {} ranges {}", new Object[]{this.description, source, StringUtils.join(ranges, ", ")});
         }
      }

      this.streamFuture = this.streamPlan.execute();
      return this.streamFuture;
   }

   public void abort(String reason) {
      if(this.streamFuture == null) {
         throw new IllegalStateException("Range streaming has not been started");
      } else {
         this.streamFuture.abort(reason);
      }
   }

   public interface ISourceFilter {
      boolean shouldInclude(InetAddress var1);
   }
}
