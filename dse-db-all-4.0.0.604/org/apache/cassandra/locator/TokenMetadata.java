package org.apache.cassandra.locator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import java.net.InetAddress;
import java.nio.ByteBuffer;
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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundTcpConnection;
import org.apache.cassandra.net.OutboundTcpConnectionPool;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BiMultiValMap;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.SortedBiMultiValMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenMetadata {
   private static final Logger logger = LoggerFactory.getLogger(TokenMetadata.class);
   private final BiMultiValMap<Token, InetAddress> tokenToEndpointMap;
   private final BiMap<InetAddress, UUID> endpointToHostIdMap;
   private final BiMultiValMap<Token, InetAddress> bootstrapTokens;
   private final BiMap<InetAddress, InetAddress> replacementToOriginal;
   private final Set<InetAddress> leavingEndpoints;
   private final ConcurrentMap<String, PendingRangeMaps> pendingRanges;
   private final Set<Pair<Token, InetAddress>> movingEndpoints;
   private final ReadWriteLock lock;
   private volatile ArrayList<Token> sortedTokens;
   protected final TokenMetadata.Topology topology;
   public final IPartitioner partitioner;
   private static final Comparator<InetAddress> inetaddressCmp = new Comparator<InetAddress>() {
      public int compare(InetAddress o1, InetAddress o2) {
         return ByteBuffer.wrap(o1.getAddress()).compareTo(ByteBuffer.wrap(o2.getAddress()));
      }
   };
   private volatile long ringVersion;
   private final AtomicReference<TokenMetadata> cachedTokenMap;

   public TokenMetadata() {
      this(SortedBiMultiValMap.create((Comparator)null, inetaddressCmp), HashBiMap.create(), new TokenMetadata.Topology(), DatabaseDescriptor.getPartitioner());
   }

   private TokenMetadata(BiMultiValMap<Token, InetAddress> tokenToEndpointMap, BiMap<InetAddress, UUID> endpointsMap, TokenMetadata.Topology topology, IPartitioner partitioner) {
      this.bootstrapTokens = new BiMultiValMap();
      this.replacementToOriginal = HashBiMap.create();
      this.leavingEndpoints = new HashSet();
      this.pendingRanges = new ConcurrentHashMap();
      this.movingEndpoints = new HashSet();
      this.lock = new ReentrantReadWriteLock(true);
      this.ringVersion = 0L;
      this.cachedTokenMap = new AtomicReference();
      this.tokenToEndpointMap = tokenToEndpointMap;
      this.topology = topology;
      this.partitioner = partitioner;
      this.endpointToHostIdMap = endpointsMap;
      this.sortedTokens = this.sortTokens();
   }

   @VisibleForTesting
   public TokenMetadata cloneWithNewPartitioner(IPartitioner newPartitioner) {
      return new TokenMetadata(this.tokenToEndpointMap, this.endpointToHostIdMap, this.topology, newPartitioner);
   }

   private ArrayList<Token> sortTokens() {
      return new ArrayList(this.tokenToEndpointMap.keySet());
   }

   public int pendingRangeChanges(InetAddress source) {
      int n;
      n = 0;
      Collection<Range<Token>> sourceRanges = this.getPrimaryRangesFor(this.getTokens(source));
      this.lock.readLock().lock();
      try {
         for (Token token : this.bootstrapTokens.keySet()) {
            for (Range<Token> range : sourceRanges) {
               if (!range.contains(token)) continue;
               ++n;
            }
         }
      }
      finally {
         this.lock.readLock().unlock();
      }
      return n;
   }

   public void updateNormalToken(Token token, InetAddress endpoint) {
      this.updateNormalTokens(Collections.singleton(token), endpoint);
   }

   public void updateNormalTokens(Collection<Token> tokens, InetAddress endpoint) {
      Multimap<InetAddress, Token> endpointTokens = HashMultimap.create();
      Iterator var4 = tokens.iterator();

      while(var4.hasNext()) {
         Token token = (Token)var4.next();
         endpointTokens.put(endpoint, token);
      }

      this.updateNormalTokens(endpointTokens);
   }

   public void updateNormalTokens(Multimap<InetAddress, Token> endpointTokens) {
      if(!endpointTokens.isEmpty()) {
         this.lock.writeLock().lock();

         try {
            boolean shouldSortTokens = false;
            Iterator var3 = endpointTokens.keySet().iterator();

            while(true) {
               if(!var3.hasNext()) {
                  if(shouldSortTokens) {
                     this.sortedTokens = this.sortTokens();
                  }
                  break;
               }

               InetAddress endpoint = (InetAddress)var3.next();
               Collection<Token> tokens = endpointTokens.get(endpoint);

               assert tokens != null && !tokens.isEmpty();

               this.bootstrapTokens.removeValue(endpoint);
               this.tokenToEndpointMap.removeValue(endpoint);
               this.topology.addEndpoint(endpoint);
               this.leavingEndpoints.remove(endpoint);
               this.replacementToOriginal.remove(endpoint);
               this.removeFromMoving(endpoint);
               Iterator var6 = tokens.iterator();

               while(var6.hasNext()) {
                  Token token = (Token)var6.next();
                  InetAddress prev = (InetAddress)this.tokenToEndpointMap.put(token, endpoint);
                  if(!endpoint.equals(prev)) {
                     if(prev != null) {
                        logger.warn("Token {} changing ownership from {} to {}", new Object[]{token, prev, endpoint});
                     }

                     shouldSortTokens = true;
                  }
               }
            }
         } finally {
            this.lock.writeLock().unlock();
         }

      }
   }

   public void updateHostId(UUID hostId, InetAddress endpoint) {
      assert hostId != null;

      assert endpoint != null;

      this.lock.writeLock().lock();

      try {
         InetAddress storedEp = (InetAddress)this.endpointToHostIdMap.inverse().get(hostId);
         if(storedEp != null && !storedEp.equals(endpoint) && FailureDetector.instance.isAlive(storedEp)) {
            throw new RuntimeException(String.format("Host ID collision between active endpoint %s and %s (id=%s)", new Object[]{storedEp, endpoint, hostId}));
         }

         UUID storedId = (UUID)this.endpointToHostIdMap.get(endpoint);
         if(storedId != null && !storedId.equals(hostId)) {
            logger.warn("Changing {}'s host ID from {} to {}", new Object[]{endpoint, storedId, hostId});
         }

         this.endpointToHostIdMap.forcePut(endpoint, hostId);
      } finally {
         this.lock.writeLock().unlock();
      }

   }

   public UUID getHostId(InetAddress endpoint) {
      this.lock.readLock().lock();

      UUID var2;
      try {
         var2 = (UUID)this.endpointToHostIdMap.get(endpoint);
      } finally {
         this.lock.readLock().unlock();
      }

      return var2;
   }

   public InetAddress getEndpointForHostId(UUID hostId) {
      this.lock.readLock().lock();

      InetAddress var2;
      try {
         var2 = (InetAddress)this.endpointToHostIdMap.inverse().get(hostId);
      } finally {
         this.lock.readLock().unlock();
      }

      return var2;
   }

   public Map<InetAddress, UUID> getEndpointToHostIdMapForReading() {
      this.lock.readLock().lock();
      try {
         HashMap<InetAddress, UUID> readMap = new HashMap<InetAddress, UUID>();
         readMap.putAll((Map<InetAddress, UUID>)this.endpointToHostIdMap);
         HashMap<InetAddress, UUID> hashMap = readMap;
         return hashMap;
      }
      finally {
         this.lock.readLock().unlock();
      }
   }

   /** @deprecated */
   @Deprecated
   public void addBootstrapToken(Token token, InetAddress endpoint) {
      this.addBootstrapTokens(Collections.singleton(token), endpoint);
   }

   public void addBootstrapTokens(Collection<Token> tokens, InetAddress endpoint) {
      this.addBootstrapTokens(tokens, endpoint, (InetAddress)null);
   }

   private void addBootstrapTokens(Collection<Token> tokens, InetAddress endpoint, InetAddress original) {
      assert tokens != null && !tokens.isEmpty();

      assert endpoint != null;

      this.lock.writeLock().lock();

      try {
         Iterator var5 = tokens.iterator();

         Token token;
         while(var5.hasNext()) {
            token = (Token)var5.next();
            InetAddress oldEndpoint = (InetAddress)this.bootstrapTokens.get(token);
            if(oldEndpoint != null && !oldEndpoint.equals(endpoint)) {
               throw new RuntimeException("Bootstrap Token collision between " + oldEndpoint + " and " + endpoint + " (token " + token);
            }

            oldEndpoint = (InetAddress)this.tokenToEndpointMap.get(token);
            if(oldEndpoint != null && !oldEndpoint.equals(endpoint) && !oldEndpoint.equals(original)) {
               throw new RuntimeException("Bootstrap Token collision between " + oldEndpoint + " and " + endpoint + " (token " + token);
            }
         }

         this.bootstrapTokens.removeValue(endpoint);
         var5 = tokens.iterator();

         while(var5.hasNext()) {
            token = (Token)var5.next();
            this.bootstrapTokens.put(token, endpoint);
         }
      } finally {
         this.lock.writeLock().unlock();
      }

   }

   public void addReplaceTokens(Collection<Token> replacingTokens, InetAddress newNode, InetAddress oldNode) {
      assert replacingTokens != null && !replacingTokens.isEmpty();

      assert newNode != null && oldNode != null;

      this.lock.writeLock().lock();

      try {
         Collection<Token> oldNodeTokens = this.tokenToEndpointMap.inverse().get(oldNode);
         if(!replacingTokens.containsAll(oldNodeTokens) || !oldNodeTokens.containsAll(replacingTokens)) {
            throw new RuntimeException(String.format("Node %s is trying to replace node %s with tokens %s with a different set of tokens %s.", new Object[]{newNode, oldNode, oldNodeTokens, replacingTokens}));
         }

         logger.debug("Replacing {} with {}", newNode, oldNode);
         this.replacementToOriginal.put(newNode, oldNode);
         this.addBootstrapTokens(replacingTokens, newNode, oldNode);
      } finally {
         this.lock.writeLock().unlock();
      }

   }

   public Optional<InetAddress> getReplacementNode(InetAddress endpoint) {
      return Optional.ofNullable(this.replacementToOriginal.inverse().get(endpoint));
   }

   public Optional<InetAddress> getReplacingNode(InetAddress endpoint) {
      return Optional.ofNullable(this.replacementToOriginal.get(endpoint));
   }

   public void removeBootstrapTokens(Collection<Token> tokens) {
      assert tokens != null && !tokens.isEmpty();

      this.lock.writeLock().lock();

      try {
         Iterator var2 = tokens.iterator();

         while(var2.hasNext()) {
            Token token = (Token)var2.next();
            this.bootstrapTokens.remove(token);
         }
      } finally {
         this.lock.writeLock().unlock();
      }

   }

   public void addLeavingEndpoint(InetAddress endpoint) {
      assert endpoint != null;

      this.lock.writeLock().lock();

      try {
         this.leavingEndpoints.add(endpoint);
      } finally {
         this.lock.writeLock().unlock();
      }

   }

   public void addMovingEndpoint(Token token, InetAddress endpoint) {
      assert endpoint != null;

      this.lock.writeLock().lock();

      try {
         this.movingEndpoints.add(Pair.create(token, endpoint));
      } finally {
         this.lock.writeLock().unlock();
      }

   }

   public void removeEndpoint(InetAddress endpoint) {
      assert endpoint != null;

      this.lock.writeLock().lock();

      try {
         this.bootstrapTokens.removeValue(endpoint);
         this.tokenToEndpointMap.removeValue(endpoint);
         this.topology.removeEndpoint(endpoint);
         this.leavingEndpoints.remove(endpoint);
         if(this.replacementToOriginal.remove(endpoint) != null) {
            logger.debug("Node {} failed during replace.", endpoint);
         }

         this.endpointToHostIdMap.remove(endpoint);
         this.sortedTokens = this.sortTokens();
         this.invalidateCachedRings();
      } finally {
         this.lock.writeLock().unlock();
      }

   }

   public void updateTopology(InetAddress endpoint) {
      assert endpoint != null;

      this.lock.writeLock().lock();

      try {
         logger.info("Updating topology for {}", endpoint);
         this.topology.updateEndpoint(endpoint);
         this.invalidateCachedRings();
      } finally {
         this.lock.writeLock().unlock();
      }

   }

   public void updateTopology() {
      this.lock.writeLock().lock();

      try {
         logger.info("Updating topology for all endpoints that have changed");
         this.topology.updateEndpoints();
         this.invalidateCachedRings();
      } finally {
         this.lock.writeLock().unlock();
      }

   }

   public void removeFromMoving(InetAddress endpoint) {
      assert endpoint != null;

      this.lock.writeLock().lock();

      try {
         Iterator var2 = this.movingEndpoints.iterator();

         while(true) {
            if(var2.hasNext()) {
               Pair<Token, InetAddress> pair = (Pair)var2.next();
               if(!((InetAddress)pair.right).equals(endpoint)) {
                  continue;
               }

               this.movingEndpoints.remove(pair);
            }

            this.invalidateCachedRings();
            return;
         }
      } finally {
         this.lock.writeLock().unlock();
      }
   }

   public Collection<Token> getTokens(InetAddress endpoint) {
      assert endpoint != null;

      this.lock.readLock().lock();

      ArrayList var2;
      try {
         assert this.isMember(endpoint);

         var2 = new ArrayList(this.tokenToEndpointMap.inverse().get(endpoint));
      } finally {
         this.lock.readLock().unlock();
      }

      return var2;
   }

   /** @deprecated */
   @Deprecated
   public Token getToken(InetAddress endpoint) {
      return (Token)this.getTokens(endpoint).iterator().next();
   }

   public boolean isMember(InetAddress endpoint) {
      assert endpoint != null;

      this.lock.readLock().lock();

      boolean var2;
      try {
         var2 = this.tokenToEndpointMap.inverse().containsKey(endpoint);
      } finally {
         this.lock.readLock().unlock();
      }

      return var2;
   }

   public boolean isLeaving(InetAddress endpoint) {
      assert endpoint != null;

      this.lock.readLock().lock();

      boolean var2;
      try {
         var2 = this.leavingEndpoints.contains(endpoint);
      } finally {
         this.lock.readLock().unlock();
      }

      return var2;
   }

   public boolean isMoving(InetAddress endpoint) {
      assert endpoint != null;

      this.lock.readLock().lock();

      boolean var8;
      try {
         Iterator var2 = this.movingEndpoints.iterator();

         while(var2.hasNext()) {
            Pair<Token, InetAddress> pair = (Pair)var2.next();
            if(((InetAddress)pair.right).equals(endpoint)) {
               boolean var4 = true;
               return var4;
            }
         }

         var8 = false;
      } finally {
         this.lock.readLock().unlock();
      }

      return var8;
   }

   public TokenMetadata cloneOnlyTokenMap() {
      this.lock.readLock().lock();

      TokenMetadata var1;
      try {
         var1 = new TokenMetadata(SortedBiMultiValMap.create(this.tokenToEndpointMap, (Comparator)null, inetaddressCmp), HashBiMap.create(this.endpointToHostIdMap), new TokenMetadata.Topology(this.topology), this.partitioner);
      } finally {
         this.lock.readLock().unlock();
      }

      return var1;
   }

   public TokenMetadata cachedOnlyTokenMap() {
      TokenMetadata tm = (TokenMetadata)this.cachedTokenMap.get();
      if(tm != null) {
         return tm;
      } else {
         synchronized(this) {
            if((tm = (TokenMetadata)this.cachedTokenMap.get()) != null) {
               return tm;
            } else {
               tm = this.cloneOnlyTokenMap();
               this.cachedTokenMap.set(tm);
               return tm;
            }
         }
      }
   }

   public TokenMetadata cloneAfterAllLeft() {
      this.lock.readLock().lock();

      TokenMetadata var1;
      try {
         var1 = removeEndpoints(this.cloneOnlyTokenMap(), this.leavingEndpoints);
      } finally {
         this.lock.readLock().unlock();
      }

      return var1;
   }

   private static TokenMetadata removeEndpoints(TokenMetadata allLeftMetadata, Set<InetAddress> leavingEndpoints) {
      Iterator var2 = leavingEndpoints.iterator();

      while(var2.hasNext()) {
         InetAddress endpoint = (InetAddress)var2.next();
         allLeftMetadata.removeEndpoint(endpoint);
      }

      return allLeftMetadata;
   }

   public TokenMetadata cloneAfterAllSettled() {
      this.lock.readLock().lock();

      try {
         TokenMetadata metadata = this.cloneOnlyTokenMap();
         Iterator var2 = this.leavingEndpoints.iterator();

         while(var2.hasNext()) {
            InetAddress endpoint = (InetAddress)var2.next();
            metadata.removeEndpoint(endpoint);
         }

         var2 = this.movingEndpoints.iterator();

         while(var2.hasNext()) {
            Pair<Token, InetAddress> pair = (Pair)var2.next();
            metadata.updateNormalToken((Token)pair.left, (InetAddress)pair.right);
         }

         TokenMetadata var7 = metadata;
         return var7;
      } finally {
         this.lock.readLock().unlock();
      }
   }

   public InetAddress getEndpoint(Token token) {
      this.lock.readLock().lock();

      InetAddress var2;
      try {
         var2 = (InetAddress)this.tokenToEndpointMap.get(token);
      } finally {
         this.lock.readLock().unlock();
      }

      return var2;
   }

   public Collection<Range<Token>> getPrimaryRangesFor(Collection<Token> tokens) {
      Collection<Range<Token>> ranges = new ArrayList(tokens.size());
      Iterator var3 = tokens.iterator();

      while(var3.hasNext()) {
         Token right = (Token)var3.next();
         ranges.add(new Range(this.getPredecessor(right), right));
      }

      return ranges;
   }

   /** @deprecated */
   @Deprecated
   public Range<Token> getPrimaryRangeFor(Token right) {
      return (Range)this.getPrimaryRangesFor(Arrays.asList(new Token[]{right})).iterator().next();
   }

   public ArrayList<Token> sortedTokens() {
      return this.sortedTokens;
   }

   public Multimap<Range<Token>, InetAddress> getPendingRangesMM(String keyspaceName) {
      Multimap<Range<Token>, InetAddress> map = HashMultimap.create();
      PendingRangeMaps pendingRangeMaps = (PendingRangeMaps)this.pendingRanges.get(keyspaceName);
      if(pendingRangeMaps != null) {
         Iterator var4 = pendingRangeMaps.iterator();

         while(var4.hasNext()) {
            Entry<Range<Token>, List<InetAddress>> entry = (Entry)var4.next();
            Range<Token> range = (Range)entry.getKey();
            Iterator var7 = ((List)entry.getValue()).iterator();

            while(var7.hasNext()) {
               InetAddress address = (InetAddress)var7.next();
               map.put(range, address);
            }
         }
      }

      return map;
   }

   public PendingRangeMaps getPendingRanges(String keyspaceName) {
      return (PendingRangeMaps)this.pendingRanges.get(keyspaceName);
   }

   public List<Range<Token>> getPendingRanges(String keyspaceName, InetAddress endpoint) {
      List<Range<Token>> ranges = new ArrayList();
      Iterator var4 = this.getPendingRangesMM(keyspaceName).entries().iterator();

      while(var4.hasNext()) {
         Entry<Range<Token>, InetAddress> entry = (Entry)var4.next();
         if(((InetAddress)entry.getValue()).equals(endpoint)) {
            ranges.add(entry.getKey());
         }
      }

      return ranges;
   }

   public void calculatePendingRanges(AbstractReplicationStrategy strategy, String keyspaceName) {
      ConcurrentMap var3 = this.pendingRanges;
      synchronized(this.pendingRanges) {
         if(this.bootstrapTokens.isEmpty() && this.leavingEndpoints.isEmpty() && this.movingEndpoints.isEmpty()) {
            if(logger.isTraceEnabled()) {
               logger.trace("No bootstrapping, leaving or moving nodes -> empty pending ranges for {}", keyspaceName);
            }

            this.pendingRanges.put(keyspaceName, new PendingRangeMaps());
         } else {
            if(logger.isDebugEnabled()) {
               logger.debug("Starting pending range calculation for {}", keyspaceName);
            }

            long startedAt = System.currentTimeMillis();
            BiMultiValMap<Token, InetAddress> bootstrapTokensClone = new BiMultiValMap();
            Set<InetAddress> leavingEndpointsClone = new HashSet();
            Set<Pair<Token, InetAddress>> movingEndpointsClone = new HashSet();
            this.lock.readLock().lock();

            TokenMetadata metadata;
            try {
               bootstrapTokensClone.putAll(this.bootstrapTokens);
               leavingEndpointsClone.addAll(this.leavingEndpoints);
               movingEndpointsClone.addAll(this.movingEndpoints);
               metadata = this.cloneOnlyTokenMap();
            } finally {
               this.lock.readLock().unlock();
            }

            this.pendingRanges.put(keyspaceName, calculatePendingRanges(strategy, metadata, bootstrapTokensClone, leavingEndpointsClone, movingEndpointsClone));
            long took = System.currentTimeMillis() - startedAt;
            if(logger.isDebugEnabled()) {
               logger.debug("Pending range calculation for {} completed (took: {}ms)", keyspaceName, Long.valueOf(took));
            }

            if(logger.isTraceEnabled()) {
               logger.trace("Calculated pending ranges for {}:\n{}", keyspaceName, this.pendingRanges.isEmpty()?"<empty>":this.printPendingRanges());
            }
         }

      }
   }

   private static PendingRangeMaps calculatePendingRanges(AbstractReplicationStrategy strategy, TokenMetadata metadata, BiMultiValMap<Token, InetAddress> bootstrapTokens, Set<InetAddress> leavingEndpoints, Set<Pair<Token, InetAddress>> movingEndpoints) {
      PendingRangeMaps newPendingRanges = new PendingRangeMaps();
      Multimap<InetAddress, Range<Token>> addressRanges = strategy.getAddressRanges(metadata);
      TokenMetadata allLeftMetadata = removeEndpoints(metadata.cloneOnlyTokenMap(), leavingEndpoints);
      Set<Range<Token>> affectedRanges = new HashSet();
      Iterator var9 = leavingEndpoints.iterator();

      while(var9.hasNext()) {
         InetAddress endpoint = (InetAddress)var9.next();
         affectedRanges.addAll(addressRanges.get(endpoint));
      }

      var9 = affectedRanges.iterator();

      Iterator var13;
      while(var9.hasNext()) {
         Range<Token> range = (Range)var9.next();
         Set<InetAddress> currentEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints((Token)range.right, metadata));
         Set<InetAddress> newEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints((Token)range.right, allLeftMetadata));
         var13 = Sets.difference(newEndpoints, currentEndpoints).iterator();

         while(var13.hasNext()) {
            InetAddress address = (InetAddress)var13.next();
            newPendingRanges.addPendingRange(range, address);
         }
      }

      Multimap<InetAddress, Token> bootstrapAddresses = bootstrapTokens.inverse();
      Iterator var29 = bootstrapAddresses.keySet().iterator();

      while(var29.hasNext()) {
         InetAddress endpoint = (InetAddress)var29.next();
         Collection<Token> tokens = bootstrapAddresses.get(endpoint);
         allLeftMetadata.updateNormalTokens(tokens, endpoint);
         var13 = strategy.getAddressRanges(allLeftMetadata).get(endpoint).iterator();

         while(var13.hasNext()) {
            Range<Token> range = (Range)var13.next();
            newPendingRanges.addPendingRange(range, endpoint);
         }

         allLeftMetadata.removeEndpoint(endpoint);
      }

      var29 = movingEndpoints.iterator();

      while(var29.hasNext()) {
         Pair<Token, InetAddress> moving = (Pair)var29.next();
         Set<Range<Token>> moveAffectedRanges = new HashSet();
         InetAddress endpoint = (InetAddress)moving.right;
         Iterator var36 = strategy.getAddressRanges(allLeftMetadata).get(endpoint).iterator();

         Range range;
         while(var36.hasNext()) {
            range = (Range)var36.next();
            moveAffectedRanges.add(range);
         }

         allLeftMetadata.updateNormalToken((Token)moving.left, endpoint);
         var36 = strategy.getAddressRanges(allLeftMetadata).get(endpoint).iterator();

         while(var36.hasNext()) {
            range = (Range)var36.next();
            moveAffectedRanges.add(range);
         }

         var36 = moveAffectedRanges.iterator();

         while(var36.hasNext()) {
            range = (Range)var36.next();
            Set<InetAddress> currentEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints((Token)range.right, metadata));
            Set<InetAddress> newEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints((Token)range.right, allLeftMetadata));
            Set<InetAddress> difference = Sets.difference(newEndpoints, currentEndpoints);
            Iterator var19 = difference.iterator();

            while(var19.hasNext()) {
               InetAddress address = (InetAddress)var19.next();
               Collection<Range<Token>> newRanges = strategy.getAddressRanges(allLeftMetadata).get(address);
               Collection<Range<Token>> oldRanges = strategy.getAddressRanges(metadata).get(address);
               newRanges.removeAll(oldRanges);
               Iterator var23 = newRanges.iterator();

               while(var23.hasNext()) {
                  Range<Token> newRange = (Range)var23.next();
                  Iterator var25 = newRange.subtractAll(oldRanges).iterator();

                  while(var25.hasNext()) {
                     Range<Token> pendingRange = (Range)var25.next();
                     newPendingRanges.addPendingRange(pendingRange, address);
                  }
               }
            }
         }

         allLeftMetadata.removeEndpoint(endpoint);
      }

      return newPendingRanges;
   }

   public Token getPredecessor(Token token) {
      List<Token> tokens = this.sortedTokens();
      int index = Collections.binarySearch(tokens, token);

      assert index >= 0 : token + " not found in " + StringUtils.join(this.tokenToEndpointMap.keySet(), ", ");

      return index == 0?(Token)tokens.get(tokens.size() - 1):(Token)tokens.get(index - 1);
   }

   public Token getSuccessor(Token token) {
      List<Token> tokens = this.sortedTokens();
      int index = Collections.binarySearch(tokens, token);

      assert index >= 0 : token + " not found in " + StringUtils.join(this.tokenToEndpointMap.keySet(), ", ");

      return index == tokens.size() - 1?(Token)tokens.get(0):(Token)tokens.get(index + 1);
   }

   public BiMultiValMap<Token, InetAddress> getBootstrapTokens() {
      this.lock.readLock().lock();

      BiMultiValMap var1;
      try {
         var1 = new BiMultiValMap(this.bootstrapTokens);
      } finally {
         this.lock.readLock().unlock();
      }

      return var1;
   }

   public Set<InetAddress> getAllEndpoints() {
      this.lock.readLock().lock();

      ImmutableSet var1;
      try {
         var1 = ImmutableSet.copyOf(this.endpointToHostIdMap.keySet());
      } finally {
         this.lock.readLock().unlock();
      }

      return var1;
   }

   public int getSizeOfAllEndpoints() {
      return this.endpointToHostIdMap.size();
   }

   public Set<InetAddress> getLeavingEndpoints() {
      this.lock.readLock().lock();

      ImmutableSet var1;
      try {
         var1 = ImmutableSet.copyOf(this.leavingEndpoints);
      } finally {
         this.lock.readLock().unlock();
      }

      return var1;
   }

   public int getSizeOfLeavingEndpoints() {
      return this.leavingEndpoints.size();
   }

   public Set<Pair<Token, InetAddress>> getMovingEndpoints() {
      this.lock.readLock().lock();

      ImmutableSet var1;
      try {
         var1 = ImmutableSet.copyOf(this.movingEndpoints);
      } finally {
         this.lock.readLock().unlock();
      }

      return var1;
   }

   public int getSizeOfMovingEndpoints() {
      return this.movingEndpoints.size();
   }

   public static int firstTokenIndex(ArrayList<Token> ring, Token start, boolean insertMin) {
      assert ring.size() > 0;

      int i = Collections.binarySearch(ring, start);
      if(i < 0) {
         i = (i + 1) * -1;
         if(i >= ring.size()) {
            i = insertMin?-1:0;
         }
      }

      return i;
   }

   public static Token firstToken(ArrayList<Token> ring, Token start) {
      return (Token)ring.get(firstTokenIndex(ring, start, false));
   }

   public static Iterator<Token> ringIterator(final ArrayList<Token> ring, final Token start, boolean includeMin) {
      if(ring.isEmpty()) {
         return (Iterator)(includeMin?Iterators.singletonIterator(start.getPartitioner().getMinimumToken()):Collections.emptyIterator());
      } else {
         final boolean insertMin = includeMin && !((Token)ring.get(0)).isMinimum();
         final int startIndex = firstTokenIndex(ring, start, insertMin);
         return new AbstractIterator<Token>() {
            int j = startIndex;

            protected Token computeNext() {
               if(this.j < -1) {
                  return (Token)this.endOfData();
               } else {
                  Token var1;
                  try {
                     if(this.j != -1) {
                        var1 = (Token)ring.get(this.j);
                        return var1;
                     }

                     var1 = start.getPartitioner().getMinimumToken();
                  } finally {
                     ++this.j;
                     if(this.j == ring.size()) {
                        this.j = insertMin?-1:0;
                     }

                     if(this.j == startIndex) {
                        this.j = -2;
                     }

                  }

                  return var1;
               }
            }
         };
      }
   }

   public void clearUnsafe() {
      this.lock.writeLock().lock();

      try {
         this.tokenToEndpointMap.clear();
         this.endpointToHostIdMap.clear();
         this.bootstrapTokens.clear();
         this.leavingEndpoints.clear();
         this.pendingRanges.clear();
         this.movingEndpoints.clear();
         this.sortedTokens.clear();
         this.topology.clear();
         this.invalidateCachedRings();
      } finally {
         this.lock.writeLock().unlock();
      }

   }

   public String toString() {
      StringBuilder sb = new StringBuilder();
      this.lock.readLock().lock();

      try {
         Multimap<InetAddress, Token> endpointToTokenMap = this.tokenToEndpointMap.inverse();
         Set<InetAddress> eps = endpointToTokenMap.keySet();
         Iterator var4;
         InetAddress ep;
         if(!eps.isEmpty()) {
            sb.append("Normal Tokens:");
            sb.append(System.getProperty("line.separator"));
            var4 = eps.iterator();

            while(var4.hasNext()) {
               ep = (InetAddress)var4.next();
               sb.append(ep);
               sb.append(':');
               sb.append(endpointToTokenMap.get(ep));
               sb.append(System.getProperty("line.separator"));
            }
         }

         if(!this.bootstrapTokens.isEmpty()) {
            sb.append("Bootstrapping Tokens:");
            sb.append(System.getProperty("line.separator"));
            var4 = this.bootstrapTokens.entrySet().iterator();

            while(var4.hasNext()) {
               Entry<Token, InetAddress> entry = (Entry)var4.next();
               sb.append(entry.getValue()).append(':').append(entry.getKey());
               sb.append(System.getProperty("line.separator"));
            }
         }

         if(!this.leavingEndpoints.isEmpty()) {
            sb.append("Leaving Endpoints:");
            sb.append(System.getProperty("line.separator"));
            var4 = this.leavingEndpoints.iterator();

            while(var4.hasNext()) {
               ep = (InetAddress)var4.next();
               sb.append(ep);
               sb.append(System.getProperty("line.separator"));
            }
         }

         if(!this.pendingRanges.isEmpty()) {
            sb.append("Pending Ranges:");
            sb.append(System.getProperty("line.separator"));
            sb.append(this.printPendingRanges());
         }
      } finally {
         this.lock.readLock().unlock();
      }

      return sb.toString();
   }

   private String printPendingRanges() {
      StringBuilder sb = new StringBuilder();
      Iterator var2 = this.pendingRanges.values().iterator();

      while(var2.hasNext()) {
         PendingRangeMaps pendingRangeMaps = (PendingRangeMaps)var2.next();
         sb.append(pendingRangeMaps.printPendingRanges());
      }

      return sb.toString();
   }

   public Collection<InetAddress> pendingEndpointsFor(Token token, String keyspaceName) {
      PendingRangeMaps pendingRangeMaps = (PendingRangeMaps)this.pendingRanges.get(keyspaceName);
      return (Collection)(pendingRangeMaps == null?Collections.emptyList():pendingRangeMaps.pendingEndpointsFor(token));
   }

   /** @deprecated */
   public Collection<InetAddress> getWriteEndpoints(Token token, String keyspaceName, Collection<InetAddress> naturalEndpoints) {
      return ImmutableList.copyOf(Iterables.concat(naturalEndpoints, this.pendingEndpointsFor(token, keyspaceName)));
   }

   public Multimap<InetAddress, Token> getEndpointToTokenMapForReading() {
      this.lock.readLock().lock();
      try {
         HashMultimap cloned = HashMultimap.create();
         for (Map.Entry<Token, InetAddress> entry : this.tokenToEndpointMap.entrySet()) {
            cloned.put((Object)entry.getValue(), (Object)entry.getKey());
         }
         HashMultimap hashMultimap = cloned;
         return hashMultimap;
      }
      finally {
         this.lock.readLock().unlock();
      }
   }

   public Map<Token, InetAddress> getNormalAndBootstrappingTokenToEndpointMap() {
      this.lock.readLock().lock();
      try {
         HashMap<Token, InetAddress> map = new HashMap<Token, InetAddress>(this.tokenToEndpointMap.size() + this.bootstrapTokens.size());
         map.putAll(this.tokenToEndpointMap);
         map.putAll(this.bootstrapTokens);
         HashMap<Token, InetAddress> hashMap = map;
         return hashMap;
      }
      finally {
         this.lock.readLock().unlock();
      }
   }

   public TokenMetadata.Topology getTopology() {
      assert this != StorageService.instance.getTokenMetadata();

      return this.topology;
   }

   public long getRingVersion() {
      return this.ringVersion;
   }

   public void invalidateCachedRings() {
      ++this.ringVersion;
      this.cachedTokenMap.set(null);
   }

   public DecoratedKey decorateKey(ByteBuffer key) {
      return this.partitioner.decorateKey(key);
   }

   public static class Topology {
      private final Multimap<String, InetAddress> dcEndpoints;
      private final Map<String, Multimap<String, InetAddress>> dcRacks;
      private final Map<InetAddress, Pair<String, String>> currentLocations;

      @VisibleForTesting
      public Topology() {
         this.dcEndpoints = HashMultimap.create();
         this.dcRacks = new HashMap();
         this.currentLocations = new HashMap();
      }

      void clear() {
         this.dcEndpoints.clear();
         this.dcRacks.clear();
         this.currentLocations.clear();
      }

      Topology(TokenMetadata.Topology other) {
         this.dcEndpoints = HashMultimap.create(other.dcEndpoints);
         this.dcRacks = new HashMap();
         Iterator var2 = other.dcRacks.keySet().iterator();

         while(var2.hasNext()) {
            String dc = (String)var2.next();
            this.dcRacks.put(dc, HashMultimap.create((Multimap)other.dcRacks.get(dc)));
         }

         this.currentLocations = new HashMap(other.currentLocations);
      }

      void addEndpoint(InetAddress ep) {
         IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
         String dc = snitch.getDatacenter(ep);
         String rack = snitch.getRack(ep);
         Pair<String, String> current = (Pair)this.currentLocations.get(ep);
         if(current != null) {
            if(((String)current.left).equals(dc) && ((String)current.right).equals(rack)) {
               return;
            }

            this.doRemoveEndpoint(ep, current);
         }

         this.doAddEndpoint(ep, dc, rack);
      }

      private void doAddEndpoint(InetAddress ep, String dc, String rack) {
         this.dcEndpoints.put(dc, ep);
         if(!this.dcRacks.containsKey(dc)) {
            this.dcRacks.put(dc, HashMultimap.create());
         }

         ((Multimap)this.dcRacks.get(dc)).put(rack, ep);
         this.currentLocations.put(ep, Pair.create(dc, rack));
      }

      void removeEndpoint(InetAddress ep) {
         if(this.currentLocations.containsKey(ep)) {
            this.doRemoveEndpoint(ep, (Pair)this.currentLocations.remove(ep));
         }
      }

      private void doRemoveEndpoint(InetAddress ep, Pair<String, String> current) {
         ((Multimap)this.dcRacks.get(current.left)).remove(current.right, ep);
         this.dcEndpoints.remove(current.left, ep);
      }

      void updateEndpoint(InetAddress ep) {
         IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
         if(snitch != null && this.currentLocations.containsKey(ep)) {
            this.updateEndpoint(ep, snitch);
         }
      }

      void updateEndpoints() {
         IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
         if(snitch != null) {
            Iterator var2 = this.currentLocations.keySet().iterator();

            while(var2.hasNext()) {
               InetAddress ep = (InetAddress)var2.next();
               this.updateEndpoint(ep, snitch);
            }

         }
      }

      private void updateEndpoint(InetAddress ep, IEndpointSnitch snitch) {
         Pair<String, String> current = (Pair)this.currentLocations.get(ep);
         String dc = snitch.getDatacenter(ep);
         String rack = snitch.getRack(ep);
         boolean currentDcIsDefault = snitch.isDefaultDC((String)current.left);
         boolean dcIsDefault = snitch.isDefaultDC(dc);
         boolean dcChanged = !dc.equals(current.left);
         boolean rackChanged = !rack.equals(current.right);
         if(dcChanged || rackChanged || currentDcIsDefault != dcIsDefault) {
            this.doRemoveEndpoint(ep, current);
            this.doAddEndpoint(ep, dc, rack);
            boolean currCompr = OutboundTcpConnection.shouldCompressConnection((String)current.left);
            boolean newCompr = OutboundTcpConnection.shouldCompressConnection(dc);
            if(currCompr != newCompr) {
               TokenMetadata.logger.debug("Resetting connection to {} due to DC compression, switching from {} to {}, changed DC from '{}'{} to '{}'{}, rack from '{}' to '{}'", new Object[]{ep, currCompr?"compressed":"uncompressed", newCompr?"compressed":"uncompressed", current.left, currentDcIsDefault?"(default)":"", dc, dcIsDefault?"(default)":"", current.right, rack});
               ((OutboundTcpConnectionPool)MessagingService.instance().getConnectionPool(ep).join()).softReset();
            }

         }
      }

      public Multimap<String, InetAddress> getDatacenterEndpoints() {
         return this.dcEndpoints;
      }

      public Map<String, Multimap<String, InetAddress>> getDatacenterRacks() {
         return this.dcRacks;
      }

      public Pair<String, String> getLocation(InetAddress addr) {
         return (Pair)this.currentLocations.get(addr);
      }
   }
}
