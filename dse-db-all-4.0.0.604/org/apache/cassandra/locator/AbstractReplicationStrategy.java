package org.apache.cassandra.locator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractReplicationStrategy {
   private static final Logger logger = LoggerFactory.getLogger(AbstractReplicationStrategy.class);
   @VisibleForTesting
   final String keyspaceName;
   private Keyspace keyspace;
   public final Map<String, String> configOptions;
   private final TokenMetadata tokenMetadata;
   private volatile long lastInvalidatedVersion = 0L;
   public IEndpointSnitch snitch;
   private final Map<Token, List<InetAddress>> cachedEndpoints = new NonBlockingHashMap();
   private final AtomicReference<AbstractReplicationStrategy.VersionedRanges> normalizedLocalRanges = new AtomicReference(new AbstractReplicationStrategy.VersionedRanges(-1L, Collections.emptyList()));

   protected AbstractReplicationStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions) {
      assert keyspaceName != null;

      assert snitch != null;

      assert tokenMetadata != null;

      this.tokenMetadata = tokenMetadata;
      this.snitch = snitch;
      this.configOptions = configOptions == null?Collections.emptyMap():configOptions;
      this.keyspaceName = keyspaceName;
   }

   public List<InetAddress> getCachedEndpoints(Token t) {
      long lastVersion = this.tokenMetadata.getRingVersion();
      if(lastVersion > this.lastInvalidatedVersion) {
         synchronized(this) {
            if(lastVersion > this.lastInvalidatedVersion) {
               logger.trace("clearing cached endpoints");
               this.cachedEndpoints.clear();
               this.lastInvalidatedVersion = lastVersion;
            }
         }
      }

      return (List)this.cachedEndpoints.get(t);
   }

   public final ArrayList<InetAddress> getNaturalEndpoints(RingPosition searchPosition) {
      return new ArrayList(this.getCachedNaturalEndpoints(searchPosition));
   }

   public List<InetAddress> getCachedNaturalEndpoints(RingPosition searchPosition) {
      Token searchToken = searchPosition.getToken();
      Token keyToken = TokenMetadata.firstToken(this.tokenMetadata.sortedTokens(), searchToken);
      List<InetAddress> endpoints = this.getCachedEndpoints(keyToken);
      if(endpoints == null) {
         TokenMetadata tm = this.tokenMetadata.cachedOnlyTokenMap();
         keyToken = TokenMetadata.firstToken(tm.sortedTokens(), searchToken);
         endpoints = ImmutableList.copyOf(this.calculateNaturalEndpoints(searchToken, tm));
         this.cachedEndpoints.put(keyToken, endpoints);
      }

      return (List)endpoints;
   }

   public abstract List<InetAddress> calculateNaturalEndpoints(Token var1, TokenMetadata var2);

   public abstract int getReplicationFactor();

   public Multimap<InetAddress, Range<Token>> getAddressRanges(TokenMetadata metadata) {
      Multimap<InetAddress, Range<Token>> map = HashMultimap.create();
      Iterator var3 = metadata.sortedTokens().iterator();

      while(var3.hasNext()) {
         Token token = (Token)var3.next();
         Range<Token> range = metadata.getPrimaryRangeFor(token);
         Iterator var6 = this.calculateNaturalEndpoints(token, metadata).iterator();

         while(var6.hasNext()) {
            InetAddress ep = (InetAddress)var6.next();
            map.put(ep, range);
         }
      }

      return map;
   }

   public Multimap<Range<Token>, InetAddress> getRangeAddresses(TokenMetadata metadata) {
      Multimap<Range<Token>, InetAddress> map = HashMultimap.create();
      Iterator var3 = metadata.sortedTokens().iterator();

      while(var3.hasNext()) {
         Token token = (Token)var3.next();
         Range<Token> range = metadata.getPrimaryRangeFor(token);
         Iterator var6 = this.calculateNaturalEndpoints(token, metadata).iterator();

         while(var6.hasNext()) {
            InetAddress ep = (InetAddress)var6.next();
            map.put(range, ep);
         }
      }

      return map;
   }

   public Multimap<InetAddress, Range<Token>> getAddressRanges() {
      return this.getAddressRanges(this.tokenMetadata.cloneOnlyTokenMap());
   }

   public Collection<Range<Token>> getNormalizedLocalRanges() {
      long lastVersion = this.tokenMetadata.getRingVersion();

      AbstractReplicationStrategy.VersionedRanges ret;
      for(ret = (AbstractReplicationStrategy.VersionedRanges)this.normalizedLocalRanges.get(); ret.ringVersion < lastVersion; ret = (AbstractReplicationStrategy.VersionedRanges)this.normalizedLocalRanges.get()) {
         List<Range<Token>> ranges = Range.normalize(this.getAddressRanges().get(FBUtilities.getBroadcastAddress()));
         if(this.normalizedLocalRanges.compareAndSet(ret, new AbstractReplicationStrategy.VersionedRanges(lastVersion, ranges))) {
            return ranges;
         }

         lastVersion = this.tokenMetadata.getRingVersion();
      }

      return ret.ranges;
   }

   public Collection<Range<Token>> getPendingAddressRanges(TokenMetadata metadata, Token pendingToken, InetAddress pendingAddress) {
      return this.getPendingAddressRanges(metadata, (Collection)Arrays.asList(new Token[]{pendingToken}), pendingAddress);
   }

   public Collection<Range<Token>> getPendingAddressRanges(TokenMetadata metadata, Collection<Token> pendingTokens, InetAddress pendingAddress) {
      TokenMetadata temp = metadata.cloneOnlyTokenMap();
      temp.updateNormalTokens(pendingTokens, pendingAddress);
      return this.getAddressRanges(temp).get(pendingAddress);
   }

   public abstract void validateOptions() throws ConfigurationException;

   public Collection<String> recognizedOptions() {
      return null;
   }

   private static AbstractReplicationStrategy createInternal(String keyspaceName, Class<? extends AbstractReplicationStrategy> strategyClass, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> strategyOptions) throws ConfigurationException {
      Class[] parameterTypes = new Class[]{String.class, TokenMetadata.class, IEndpointSnitch.class, Map.class};

      try {
         Constructor<? extends AbstractReplicationStrategy> constructor = strategyClass.getConstructor(parameterTypes);
         AbstractReplicationStrategy strategy = (AbstractReplicationStrategy)constructor.newInstance(new Object[]{keyspaceName, tokenMetadata, snitch, strategyOptions});
         return strategy;
      } catch (InvocationTargetException var9) {
         Throwable targetException = var9.getTargetException();
         throw new ConfigurationException(targetException.getMessage(), targetException);
      } catch (Exception var10) {
         throw new ConfigurationException("Error constructing replication strategy class", var10);
      }
   }

   public static AbstractReplicationStrategy createReplicationStrategy(String keyspaceName, Class<? extends AbstractReplicationStrategy> strategyClass, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> strategyOptions) {
      AbstractReplicationStrategy strategy = createInternal(keyspaceName, strategyClass, tokenMetadata, snitch, strategyOptions);

      try {
         strategy.validateExpectedOptions();
      } catch (ConfigurationException var7) {
         logger.warn("Ignoring {}", var7.getMessage());
      }

      strategy.validateOptions();
      return strategy;
   }

   public static void validateReplicationStrategy(String keyspaceName, Class<? extends AbstractReplicationStrategy> strategyClass, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> strategyOptions) throws ConfigurationException {
      AbstractReplicationStrategy strategy = createInternal(keyspaceName, strategyClass, tokenMetadata, snitch, strategyOptions);
      strategy.validateExpectedOptions();
      strategy.validateOptions();
   }

   public static Class<AbstractReplicationStrategy> getClass(String cls) throws ConfigurationException {
      String className = cls.contains(".")?cls:"org.apache.cassandra.locator." + cls;
      Class<AbstractReplicationStrategy> strategyClass = FBUtilities.classForName(className, "replication strategy");
      if(!AbstractReplicationStrategy.class.isAssignableFrom(strategyClass)) {
         throw new ConfigurationException(String.format("Specified replication strategy class (%s) is not derived from AbstractReplicationStrategy", new Object[]{className}));
      } else {
         return strategyClass;
      }
   }

   public boolean hasSameSettings(AbstractReplicationStrategy other) {
      return this.getClass().equals(other.getClass()) && this.getReplicationFactor() == other.getReplicationFactor();
   }

   protected void validateReplicationFactor(String rf) throws ConfigurationException {
      try {
         if(Integer.parseInt(rf) < 0) {
            throw new ConfigurationException("Replication factor must be non-negative; found " + rf);
         }
      } catch (NumberFormatException var3) {
         throw new ConfigurationException("Replication factor must be numeric; found " + rf);
      }
   }

   protected void validateExpectedOptions() throws ConfigurationException {
      Collection expectedOptions = this.recognizedOptions();
      if(expectedOptions != null) {
         Iterator var2 = this.configOptions.keySet().iterator();

         String key;
         do {
            if(!var2.hasNext()) {
               return;
            }

            key = (String)var2.next();
         } while(expectedOptions.contains(key));

         throw new ConfigurationException(String.format("Unrecognized strategy option {%s} passed to %s for keyspace %s", new Object[]{key, this.getClass().getSimpleName(), this.keyspaceName}));
      }
   }

   public boolean isPartitioned() {
      return true;
   }

   private static final class VersionedRanges {
      final long ringVersion;
      final List<Range<Token>> ranges;

      public VersionedRanges(long ringVersion, List<Range<Token>> ranges) {
         this.ringVersion = ringVersion;
         this.ranges = ranges;
      }
   }
}
