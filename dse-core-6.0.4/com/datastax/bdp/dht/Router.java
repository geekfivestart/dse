package com.datastax.bdp.dht;

import com.datastax.bdp.dht.endpoint.Endpoint;
import com.datastax.bdp.dht.endpoint.SeededComparator;
import com.datastax.bdp.util.Addresses;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Router implements RouterMXBean, IEndpointStateChangeSubscriber {
   public static final String EXCLUDED_HOSTS_FILE = "exclude.hosts";
   private static final Logger logger = LoggerFactory.getLogger(Router.class);
   private volatile Router.State state = new Router.State(null);
   private final Set<Router.UpdateCallback> updateCallbacks = Sets.newConcurrentHashSet();
   private final CassandraMetricsRegistry metrics;
   private final ExecutorService asyncStateUpdater;
   private volatile int updatesScheduled;
   private static final AtomicIntegerFieldUpdater<Router> updatesScheduledUpdater = AtomicIntegerFieldUpdater.newUpdater(Router.class, "updatesScheduled");
   private final Callable<Set<String>> keyspacesProvider;
   private final Predicate<InetAddress> endpointsFilter;

   public Router(Callable<Set<String>> keyspacesProvider, Predicate<InetAddress> endpointsFilter) {
      this.metrics = CassandraMetricsRegistry.Metrics;
      this.asyncStateUpdater = Executors.newSingleThreadExecutor((new ThreadFactoryBuilder()).setDaemon(true).setNameFormat("Router async state updater").build());
      this.keyspacesProvider = keyspacesProvider;
      this.endpointsFilter = endpointsFilter;
   }

   public RoutingPlan route(String keyspace, SeededComparator<Endpoint> endpointComparator, @Nullable RefiningFilter<Range<Token>> rangeFilter, SetCoverFinder.Kind coverFinderKind) {
      SetCoverFinder<Endpoint, Range<Token>> coverFinder = (SetCoverFinder)this.state.keyspaceCoverFinders.get(keyspace);
      if(null == coverFinder) {
         throw new IllegalArgumentException("Unable to send message: keyspace " + keyspace + " is not managed by the DHT Router");
      } else {
         SetCoverResult<Endpoint, Range<Token>> result = this.calculateSetCover(coverFinder, endpointComparator, rangeFilter, coverFinderKind);
         Set<Endpoint> endpoints = this.makeEndpointsFromShards(result.getCover());
         return new RoutingPlan(endpoints, result.getUncovered());
      }
   }

   public RoutingPlan reroute(String keyspace, Set<String> failedEndpoints, Set<Range<Token>> failedRanges, SeededComparator<Endpoint> endpointComparator, @Nullable RefiningFilter<Range<Token>> rangeFilter, SetCoverFinder.Kind setCoverFinder) {
      Collection<Endpoint> liveEndpoints = Collections2.filter((Collection)this.state.keyspaceEndpoints.get(keyspace), (input) -> {
         return !failedEndpoints.contains(input.getAddress().getHostAddress()) && !failedEndpoints.contains(input.getAddress().getCanonicalHostName());
      });
      logger.debug("Live endpoints: {}", liveEndpoints);
      SetCoverFinder<Endpoint, Range<Token>> coverFinder = new SetCoverFinder(failedRanges, liveEndpoints, new Endpoint.GetProvidedRanges());
      SetCoverResult<Endpoint, Range<Token>> result = this.calculateSetCover(coverFinder, endpointComparator, rangeFilter, setCoverFinder);
      Set<Endpoint> endpoints = this.makeEndpointsFromShards(result.getCover());
      return new RoutingPlan(endpoints, result.getUncovered());
   }

   public void addUpdateCallback(Router.UpdateCallback callback) {
      this.updateCallbacks.add(callback);
   }

   public void removeUpdateCallback(Router.UpdateCallback callback) {
      this.updateCallbacks.remove(callback);
   }

   public void update() {
      this.refresh(false);
   }

   public void shutdown() {
      this.asyncStateUpdater.shutdown();
   }

   public Map<String, String> getEndpointsLoad(String keyspace) {
      List<Endpoint> endpoints = (List)this.state.keyspaceEndpoints.get(keyspace);
      if(endpoints == null) {
         return Collections.emptyMap();
      } else {
         Map<String, String> result = Maps.newHashMap();
         Iterator var4 = endpoints.iterator();

         while(var4.hasNext()) {
            Endpoint endpoint = (Endpoint)var4.next();
            double[] rates = endpoint.getLoadRate();
            result.put(endpoint.getAddress().getHostAddress(), MoreObjects.toStringHelper("EWMA").add("1-min", rates[0]).add("5-min", rates[1]).add("15-min", rates[2]).toString());
         }

         return result;
      }
   }

   public List<String> getEndpoints(String keyspace) {
      List<Endpoint> endpoints = (List)this.state.keyspaceEndpoints.get(keyspace);
      return (List)(endpoints == null?Collections.emptyList():Lists.newLinkedList(Lists.transform(endpoints, (input) -> {
         return input.getAddress().getHostAddress();
      })));
   }

   public void refreshEndpoints() {
      this.refresh(false);
   }

   public void onChange(InetAddress endpoint, ApplicationState apState, VersionedValue value) {
      boolean updating = false;
      if(apState != null) {
         switch(null.$SwitchMap$org$apache$cassandra$gms$ApplicationState[apState.ordinal()]) {
         case 1:
            if(value != null && (value.value.startsWith("NORMAL") || value.value.startsWith("LEAVING") || value.value.startsWith("LEFT"))) {
               updating = true;
               this.refresh(true);
            }
            break;
         case 2:
            updating = true;
            this.refresh(true);
         }
      }

      if(updating) {
         logger.info("Updating shards state due to endpoint {} changing state {}={}", new Object[]{endpoint, apState, value.value});
      }

   }

   public void onDead(InetAddress endpoint, EndpointState epState) {
      if(!endpoint.equals(Addresses.Internode.getBroadcastAddress())) {
         logger.info("Updating shards state due to endpoint {} being dead", endpoint);
         this.refresh(true);
      }

   }

   public void onRemove(InetAddress endpoint) {
      if(!endpoint.equals(Addresses.Internode.getBroadcastAddress())) {
         logger.info("Updating shards state due to endpoint {} being removed.", endpoint);
         this.refresh(true);
      }

   }

   public void onJoin(InetAddress endpoint, EndpointState epState) {
      if(!endpoint.equals(Addresses.Internode.getBroadcastAddress())) {
         this.onChange(endpoint, ApplicationState.STATUS, epState.getApplicationState(ApplicationState.STATUS));
      }

   }

   public void onAlive(InetAddress endpoint, EndpointState epState) {
      if(!endpoint.equals(Addresses.Internode.getBroadcastAddress())) {
         this.onChange(endpoint, ApplicationState.STATUS, epState.getApplicationState(ApplicationState.STATUS));
      }

   }

   public void beforeChange(InetAddress address, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {
   }

   public void onRestart(InetAddress endpoint, EndpointState epState) {
   }

   @VisibleForTesting
   public List<Endpoint> getEndpointContainers(String keyspace) {
      List<Endpoint> endpoints = (List)this.state.keyspaceEndpoints.get(keyspace);
      return (List)(endpoints == null?Collections.emptyList():Lists.newLinkedList(Lists.transform(endpoints, Endpoint::<init>)));
   }

   private SetCoverResult<Endpoint, Range<Token>> calculateSetCover(SetCoverFinder<Endpoint, Range<Token>> coverFinder, SeededComparator<Endpoint> endpointComparator, @Nullable RefiningFilter<Range<Token>> rangeFilter, SetCoverFinder.Kind coverFinderKind) {
      return coverFinder.findSetCover(coverFinderKind.strategy(endpointComparator), rangeFilter);
   }

   private void refresh(boolean async) {
      if(async) {
         if(updatesScheduledUpdater.compareAndSet(this, 0, 1)) {
            this.asyncStateUpdater.submit(() -> {
               updatesScheduledUpdater.set(this, 0);
               this.updateState();
            });
         }
      } else {
         this.updateState();
      }

   }

   private Set<Endpoint> makeEndpointsFromShards(Map<Endpoint, List<Range<Token>>> shards) {
      Set<Endpoint> results = Sets.newHashSet();
      Iterator var3 = shards.entrySet().iterator();

      while(var3.hasNext()) {
         Entry<Endpoint, List<Range<Token>>> shard = (Entry)var3.next();
         Endpoint endpoint = (Endpoint)shard.getKey();
         endpoint.markLoadRate();
         InetAddress contactAddress = Addresses.Internode.getPreferredHost(endpoint.getAddress());
         logger.debug("Contact address for routing endpoint {} is: {}", endpoint.getAddress(), contactAddress);
         Endpoint result = new Endpoint(contactAddress, endpoint.getProvidedTokenRanges());
         result.setUsedTokenRanges((Collection)shard.getValue());
         results.add(result);
      }

      return results;
   }

   private void updateEndpoints(Router.State state, String keyspace, Map<Range<Token>, Iterable<InetAddress>> rangeToEndpoints) {
      Map<InetAddress, Set<Range<Token>>> endpointToRanges = Maps.newHashMap();
      Set<String> excludedHosts = this.getExcludedHosts();
      Iterator var6 = rangeToEndpoints.entrySet().iterator();

      InetAddress endpoint;
      label43:
      while(var6.hasNext()) {
         Entry<Range<Token>, Iterable<InetAddress>> rangeEntry = (Entry)var6.next();
         Iterator var8 = ((Iterable)rangeEntry.getValue()).iterator();

         while(true) {
            while(true) {
               if(!var8.hasNext()) {
                  continue label43;
               }

               endpoint = (InetAddress)var8.next();
               boolean alive = FailureDetector.instance.isAlive(endpoint);
               boolean excluded = excludedHosts.contains(endpoint.getHostAddress());
               Range<Token> range = (Range)rangeEntry.getKey();
               if(alive && !excluded) {
                  logger.debug("Adding live routing endpoint {} for range {}", endpoint, range);
                  if(endpointToRanges.containsKey(endpoint)) {
                     ((Set)endpointToRanges.get(endpoint)).add(range);
                  } else {
                     Set<Range<Token>> ranges = Sets.newHashSet();
                     ranges.add(range);
                     endpointToRanges.put(endpoint, ranges);
                  }
               } else if(excluded) {
                  logger.debug("Discarded excluded routing endpoint {} for range {}", endpoint, range);
               } else {
                  logger.debug("Discarded dead routing endpoint {} for range {}", endpoint, range);
               }
            }
         }
      }

      List<Endpoint> endpoints = Lists.newArrayListWithCapacity(endpointToRanges.size());
      Iterator var15 = endpointToRanges.entrySet().iterator();

      while(var15.hasNext()) {
         Entry<InetAddress, Set<Range<Token>>> entry = (Entry)var15.next();
         endpoint = (InetAddress)entry.getKey();
         Set<Range<Token>> ranges = (Set)entry.getValue();
         Endpoint endpoint = new Endpoint(endpoint, ranges);
         endpoint.initLoadRate(this.metrics);
         endpoints.add(endpoint);
      }

      state.keyspaceEndpoints.put(keyspace, endpoints);
   }

   private Map<Range<Token>, Iterable<InetAddress>> getFilteredRangeToAddressMap(String keyspace) {
      Map<Range<Token>, List<InetAddress>> localDCRanges = StorageService.instance.getRangeToAddressMapInLocalDC(keyspace);
      java.util.function.Function<List<InetAddress>, Iterable<InetAddress>> filterNodes = (candidates) -> {
         return (List)candidates.stream().filter(this.endpointsFilter).collect(Collectors.toList());
      };
      filterNodes.getClass();
      return Maps.transformValues(localDCRanges, filterNodes::apply);
   }

   private Set<String> getExcludedHosts() {
      try {
         InputStream excludes = Thread.currentThread().getContextClassLoader().getResourceAsStream("exclude.hosts");
         Set<String> result = new HashSet();
         if(excludes != null) {
            try {
               BufferedReader reader = new BufferedReader(new InputStreamReader(excludes));

               for(String host = reader.readLine(); host != null; host = reader.readLine()) {
                  result.add(host);
               }
            } finally {
               excludes.close();
            }
         }

         return result;
      } catch (IOException var9) {
         logger.warn("Error reading file: exclude.hosts", var9);
         return Collections.emptySet();
      }
   }

   private void updateState() {
      Router.State state = new Router.State(null);

      try {
         Iterator var2 = ((Set)this.keyspacesProvider.call()).iterator();

         while(var2.hasNext()) {
            String keyspace = (String)var2.next();
            Map<Range<Token>, Iterable<InetAddress>> rangeToEndpoints = this.getFilteredRangeToAddressMap(keyspace);
            state.keyspaceRanges.put(keyspace, rangeToEndpoints.keySet());
            this.updateEndpoints(state, keyspace, rangeToEndpoints);
            Iterator var5 = this.updateCallbacks.iterator();

            while(var5.hasNext()) {
               Router.UpdateCallback callback = (Router.UpdateCallback)var5.next();
               callback.onUpdate(rangeToEndpoints);
            }

            SetCoverFinder<Endpoint, Range<Token>> coverFinder = new SetCoverFinder((Set)state.keyspaceRanges.get(keyspace), (Collection)state.keyspaceEndpoints.get(keyspace), new Endpoint.GetProvidedRanges());
            state.keyspaceCoverFinders.put(keyspace, coverFinder);
         }

         this.state = state;
      } catch (Exception var7) {
         logger.warn(var7.getMessage(), var7);
      }

   }

   public interface UpdateCallback {
      void onUpdate(Map<Range<Token>, Iterable<InetAddress>> var1);
   }

   private static final class State {
      final Map<String, Set<Range<Token>>> keyspaceRanges;
      final Map<String, List<Endpoint>> keyspaceEndpoints;
      final Map<String, SetCoverFinder<Endpoint, Range<Token>>> keyspaceCoverFinders;

      private State() {
         this.keyspaceRanges = new HashMap();
         this.keyspaceEndpoints = new HashMap();
         this.keyspaceCoverFinders = new HashMap();
      }
   }
}
