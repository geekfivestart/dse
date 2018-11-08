package org.apache.cassandra.dht.tokenallocator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Map.Entry;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;

public abstract class TokenAllocatorBase<Unit> implements TokenAllocator<Unit> {
   static final double MIN_INITIAL_SPLITS_RATIO = 1.0D - 1.0D / Math.sqrt(5.0D);
   static final double MAX_INITIAL_SPLITS_RATIO;
   final NavigableMap<Token, Unit> sortedTokens;
   final ReplicationStrategy<Unit> strategy;
   final IPartitioner partitioner;

   protected TokenAllocatorBase(NavigableMap<Token, Unit> sortedTokens, ReplicationStrategy<Unit> strategy, IPartitioner partitioner) {
      this.sortedTokens = sortedTokens;
      this.strategy = strategy;
      this.partitioner = partitioner;
   }

   public abstract int getReplicas();

   protected Map<Unit, TokenAllocatorBase.UnitInfo<Unit>> createUnitInfos(Map<Object, TokenAllocatorBase.GroupInfo> groups) {
      Map<Unit, TokenAllocatorBase.UnitInfo<Unit>> map = Maps.newHashMap();

      TokenAllocatorBase.UnitInfo ni;
      for(Iterator var3 = this.sortedTokens.values().iterator(); var3.hasNext(); ++ni.tokenCount) {
         Unit n = var3.next();
         ni = (TokenAllocatorBase.UnitInfo)map.get(n);
         if(ni == null) {
            map.put(n, ni = new TokenAllocatorBase.UnitInfo(n, 0.0D, groups, this.strategy));
         }
      }

      return map;
   }

   private Entry<Token, Unit> mapEntryFor(Token t) {
      Entry<Token, Unit> en = this.sortedTokens.floorEntry(t);
      if(en == null) {
         en = this.sortedTokens.lastEntry();
      }

      return en;
   }

   Unit unitFor(Token t) {
      return this.mapEntryFor(t).getValue();
   }

   private static <Unit> TokenAllocatorBase.GroupInfo getGroup(Unit unit, Map<Object, TokenAllocatorBase.GroupInfo> groupMap, ReplicationStrategy<Unit> strategy) {
      Object groupClass = strategy.getGroup(unit);
      TokenAllocatorBase.GroupInfo group = (TokenAllocatorBase.GroupInfo)groupMap.get(groupClass);
      if(group == null) {
         groupMap.put(groupClass, group = new TokenAllocatorBase.GroupInfo(groupClass));
      }

      return group;
   }

   Collection<Token> generateSplits(Unit newUnit, int numTokens) {
      return this.generateSplits(newUnit, numTokens, MIN_INITIAL_SPLITS_RATIO, MAX_INITIAL_SPLITS_RATIO);
   }

   Collection<Token> generateSplits(Unit newUnit, int numTokens, double minRatio, double maxRatio) {
      Random random = new Random((long)this.sortedTokens.size());
      double potentialRatioGrowth = maxRatio - minRatio;
      List<Token> tokens = Lists.newArrayListWithExpectedSize(numTokens);
      Token prev;
      if(this.sortedTokens.isEmpty()) {
         prev = this.partitioner.getRandomToken();
         tokens.add(prev);
         this.sortedTokens.put(prev, newUnit);
      }

      while(tokens.size() < numTokens) {
         prev = (Token)this.sortedTokens.lastKey();
         double maxsz = 0.0D;
         Token t1 = null;
         Token t2 = null;

         Token curr;
         for(Iterator var16 = this.sortedTokens.keySet().iterator(); var16.hasNext(); prev = curr) {
            curr = (Token)var16.next();
            double sz = prev.size(curr);
            if(sz > maxsz) {
               maxsz = sz;
               t1 = prev;
               t2 = curr;
            }
         }

         assert t1 != null;

         Token t = this.partitioner.split(t1, t2, Math.min(1.0D, minRatio + potentialRatioGrowth * random.nextDouble()));
         tokens.add(t);
         this.sortedTokens.put(t, newUnit);
      }

      return tokens;
   }

   static {
      MAX_INITIAL_SPLITS_RATIO = MIN_INITIAL_SPLITS_RATIO + 0.075D;
   }

   static class Weighted<T> implements Comparable<TokenAllocatorBase.Weighted<T>> {
      final double weight;
      final T value;

      public Weighted(double weight, T value) {
         this.weight = weight;
         this.value = value;
      }

      public int compareTo(TokenAllocatorBase.Weighted<T> o) {
         int cmp = Double.compare(o.weight, this.weight);
         return cmp;
      }

      public String toString() {
         return String.format("%s<%s>", new Object[]{this.value, Double.valueOf(this.weight)});
      }
   }

   static class TokenInfo<Unit> extends TokenAllocatorBase.BaseTokenInfo<Unit, TokenAllocatorBase.TokenInfo<Unit>> {
      public TokenInfo(Token token, TokenAllocatorBase.UnitInfo<Unit> owningUnit) {
         super(token, owningUnit);
      }

      TokenAllocatorBase.TokenInfo<Unit> prevInRing() {
         return (TokenAllocatorBase.TokenInfo)this.prev;
      }
   }

   static class BaseTokenInfo<Unit, T extends TokenAllocatorBase.BaseTokenInfo<Unit, T>> extends TokenAllocatorBase.CircularList<T> {
      final Token token;
      final TokenAllocatorBase.UnitInfo<Unit> owningUnit;
      Token replicationStart;
      Token replicationThreshold;
      double replicatedOwnership = 0.0D;

      public BaseTokenInfo(Token token, TokenAllocatorBase.UnitInfo<Unit> owningUnit) {
         super(null);
         this.token = token;
         this.owningUnit = owningUnit;
      }

      public String toString() {
         return String.format("%s(%s)", new Object[]{this.token, this.owningUnit});
      }

      TokenAllocatorBase.TokenInfo<Unit> prevInRing() {
         return null;
      }
   }

   private static class CircularList<T extends TokenAllocatorBase.CircularList<T>> {
      T prev;
      T next;

      private CircularList() {
      }

      T insertAfter(T head, T unit) {
         if(head == null) {
            return this.prev = this.next = this;
         } else {
            assert unit != null;

            assert unit.next != null;

            this.prev = unit;
            this.next = unit.next;
            this.prev.next = this;
            this.next.prev = this;
            return head;
         }
      }

      T removeFrom(T head) {
         this.next.prev = this.prev;
         this.prev.next = this.next;
         return this == head?(this == this.next?null:this.next):head;
      }
   }

   static class UnitInfo<Unit> {
      final Unit unit;
      final TokenAllocatorBase.GroupInfo group;
      double ownership;
      int tokenCount;
      TokenAllocatorBase.UnitInfo<Unit> prevUsed;
      double adjustedOwnership;

      private UnitInfo(Unit unit, TokenAllocatorBase.GroupInfo group) {
         this.unit = unit;
         this.group = group;
         this.tokenCount = 0;
      }

      public UnitInfo(Unit unit, double ownership, Map<Object, TokenAllocatorBase.GroupInfo> groupMap, ReplicationStrategy<Unit> strategy) {
         this(unit, TokenAllocatorBase.getGroup(unit, groupMap, strategy));
         this.ownership = ownership;
      }

      public String toString() {
         return String.format("%s%s(%.2e)%s", new Object[]{this.unit, this.unit == this.group.group?(this.group.prevSeen != null?"*":""):":" + this.group.toString(), Double.valueOf(this.ownership), this.prevUsed != null?(this.prevUsed == this?"#":"->" + this.prevUsed.toString()):""});
      }
   }

   static class GroupInfo {
      final Object group;
      TokenAllocatorBase.GroupInfo prevSeen = null;
      TokenAllocatorBase.GroupInfo prevPopulate = null;
      static TokenAllocatorBase.GroupInfo TERMINATOR = new TokenAllocatorBase.GroupInfo((Object)null);

      public GroupInfo(Object group) {
         this.group = group;
      }

      public String toString() {
         return this.group.toString() + (this.prevSeen != null?"*":"");
      }
   }
}
