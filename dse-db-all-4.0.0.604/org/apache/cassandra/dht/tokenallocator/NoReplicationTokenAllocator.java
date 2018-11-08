package org.apache.cassandra.dht.tokenallocator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Map.Entry;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;

public class NoReplicationTokenAllocator<Unit> extends TokenAllocatorBase<Unit> {
   PriorityQueue<TokenAllocatorBase.Weighted<TokenAllocatorBase.UnitInfo<Unit>>> sortedUnits = Queues.newPriorityQueue();
   Map<Unit, PriorityQueue<TokenAllocatorBase.Weighted<TokenAllocatorBase.TokenInfo<Unit>>>> tokensInUnits = Maps.newHashMap();
   private static final double MAX_TAKEOVER_RATIO = 0.9D;
   private static final double MIN_TAKEOVER_RATIO = 0.09999999999999998D;

   public NoReplicationTokenAllocator(NavigableMap<Token, Unit> sortedTokens, ReplicationStrategy<Unit> strategy, IPartitioner partitioner) {
      super(sortedTokens, strategy, partitioner);
   }

   private TokenAllocatorBase.TokenInfo<Unit> createTokenInfos(Map<Unit, TokenAllocatorBase.UnitInfo<Unit>> units) {
      if(units.isEmpty()) {
         return null;
      } else {
         TokenAllocatorBase.TokenInfo<Unit> prev = null;
         TokenAllocatorBase.TokenInfo<Unit> first = null;

         TokenAllocatorBase.TokenInfo ti;
         for(Iterator var4 = this.sortedTokens.entrySet().iterator(); var4.hasNext(); prev = ti) {
            Entry<Token, Unit> en = (Entry)var4.next();
            Token t = (Token)en.getKey();
            TokenAllocatorBase.UnitInfo<Unit> ni = (TokenAllocatorBase.UnitInfo)units.get(en.getValue());
            ti = new TokenAllocatorBase.TokenInfo(t, ni);
            first = (TokenAllocatorBase.TokenInfo)ti.insertAfter(first, prev);
         }

         TokenAllocatorBase.TokenInfo<Unit> curr = first;
         this.tokensInUnits.clear();
         this.sortedUnits.clear();

         do {
            this.populateTokenInfoAndAdjustUnit(curr);
            curr = (TokenAllocatorBase.TokenInfo)curr.next;
         } while(curr != first);

         Iterator var10 = units.values().iterator();

         while(var10.hasNext()) {
            TokenAllocatorBase.UnitInfo<Unit> unitInfo = (TokenAllocatorBase.UnitInfo)var10.next();
            this.sortedUnits.add(new TokenAllocatorBase.Weighted(unitInfo.ownership, unitInfo));
         }

         return first;
      }
   }

   protected void createTokenInfos() {
      this.createTokenInfos(this.createUnitInfos(Maps.newHashMap()));
   }

   private void populateTokenInfoAndAdjustUnit(TokenAllocatorBase.TokenInfo<Unit> token) {
      token.replicationStart = token.prevInRing().token;
      token.replicationThreshold = token.token;
      token.replicatedOwnership = token.replicationStart.size(token.token);
      token.owningUnit.ownership += token.replicatedOwnership;
      PriorityQueue<TokenAllocatorBase.Weighted<TokenAllocatorBase.TokenInfo<Unit>>> unitTokens = (PriorityQueue)this.tokensInUnits.get(token.owningUnit.unit);
      if(unitTokens == null) {
         unitTokens = Queues.newPriorityQueue();
         this.tokensInUnits.put(token.owningUnit.unit, unitTokens);
      }

      unitTokens.add(new TokenAllocatorBase.Weighted(token.replicatedOwnership, token));
   }

   public Collection<Token> addUnit(Unit newUnit, int numTokens) {
      assert !this.tokensInUnits.containsKey(newUnit);

      Map<Object, TokenAllocatorBase.GroupInfo> groups = Maps.newHashMap();
      TokenAllocatorBase.UnitInfo<Unit> newUnitInfo = new TokenAllocatorBase.UnitInfo(newUnit, 0.0D, groups, this.strategy);
      Map<Unit, TokenAllocatorBase.UnitInfo<Unit>> unitInfos = this.createUnitInfos(groups);
      if(unitInfos.isEmpty()) {
         return this.generateSplits(newUnit, numTokens);
      } else if(numTokens > this.sortedTokens.size()) {
         return this.generateSplits(newUnit, numTokens);
      } else {
         TokenAllocatorBase.TokenInfo<Unit> head = this.createTokenInfos(unitInfos);
         double targetAverage = 0.0D;
         double sum = 0.0D;
         List<TokenAllocatorBase.Weighted<TokenAllocatorBase.UnitInfo<Unit>>> unitsToChange = new ArrayList();

         for(int i = 0; i < numTokens; ++i) {
            TokenAllocatorBase.Weighted<TokenAllocatorBase.UnitInfo<Unit>> unit = (TokenAllocatorBase.Weighted)this.sortedUnits.peek();
            if(unit == null) {
               break;
            }

            sum += unit.weight;
            double average = sum / (double)(unitsToChange.size() + 2);
            if(unit.weight <= average) {
               break;
            }

            this.sortedUnits.remove();
            unitsToChange.add(unit);
            targetAverage = average;
         }

         List<Token> newTokens = Lists.newArrayListWithCapacity(numTokens);
         int nr = 0;

         for(Iterator var32 = unitsToChange.iterator(); var32.hasNext(); ++nr) {
            TokenAllocatorBase.Weighted<TokenAllocatorBase.UnitInfo<Unit>> unit = (TokenAllocatorBase.Weighted)var32.next();
            int tokensToChange = numTokens / unitsToChange.size() + (nr < numTokens % unitsToChange.size()?1:0);
            Queue<TokenAllocatorBase.Weighted<TokenAllocatorBase.TokenInfo<Unit>>> unitTokens = (Queue)this.tokensInUnits.get(((TokenAllocatorBase.UnitInfo)unit.value).unit);
            List<TokenAllocatorBase.Weighted<TokenAllocatorBase.TokenInfo<Unit>>> tokens = Lists.newArrayListWithCapacity(tokensToChange);
            double workWeight = 0.0D;

            for(int i = 0; i < tokensToChange; ++i) {
               TokenAllocatorBase.Weighted<TokenAllocatorBase.TokenInfo<Unit>> wt = (TokenAllocatorBase.Weighted)unitTokens.remove();
               tokens.add(wt);
               workWeight += wt.weight;
               ((TokenAllocatorBase.UnitInfo)unit.value).ownership -= wt.weight;
            }

            double toTakeOver = unit.weight - targetAverage;
            Iterator var23 = tokens.iterator();

            while(var23.hasNext()) {
               TokenAllocatorBase.Weighted<TokenAllocatorBase.TokenInfo<Unit>> wt = (TokenAllocatorBase.Weighted)var23.next();
               double slice;
               if(toTakeOver < workWeight) {
                  slice = toTakeOver / workWeight;
                  if(slice < 0.09999999999999998D) {
                     slice = 0.09999999999999998D;
                  }

                  if(slice > 0.9D) {
                     slice = 0.9D;
                  }
               } else {
                  slice = 0.9D;
               }

               Token token = this.partitioner.split(((TokenAllocatorBase.TokenInfo)wt.value).prevInRing().token, ((TokenAllocatorBase.TokenInfo)wt.value).token, Math.min(1.0D, slice));
               this.sortedTokens.put(token, newUnit);
               TokenAllocatorBase.TokenInfo<Unit> ti = new TokenAllocatorBase.TokenInfo(token, newUnitInfo);
               ti.insertAfter(head, ((TokenAllocatorBase.TokenInfo)wt.value).prevInRing());
               this.populateTokenInfoAndAdjustUnit(ti);
               this.populateTokenInfoAndAdjustUnit((TokenAllocatorBase.TokenInfo)wt.value);
               newTokens.add(token);
            }

            this.sortedUnits.add(new TokenAllocatorBase.Weighted(((TokenAllocatorBase.UnitInfo)unit.value).ownership, unit.value));
         }

         this.sortedUnits.add(new TokenAllocatorBase.Weighted(newUnitInfo.ownership, newUnitInfo));
         return newTokens;
      }
   }

   void removeUnit(Unit n) {
      Iterator it = this.sortedUnits.iterator();

      while(it.hasNext()) {
         if(((TokenAllocatorBase.UnitInfo)((TokenAllocatorBase.Weighted)it.next()).value).unit.equals(n)) {
            it.remove();
            break;
         }
      }

      PriorityQueue<TokenAllocatorBase.Weighted<TokenAllocatorBase.TokenInfo<Unit>>> tokenInfos = (PriorityQueue)this.tokensInUnits.remove(n);
      Collection<Token> tokens = Lists.newArrayListWithCapacity(tokenInfos.size());
      Iterator var5 = tokenInfos.iterator();

      while(var5.hasNext()) {
         TokenAllocatorBase.Weighted<TokenAllocatorBase.TokenInfo<Unit>> tokenInfo = (TokenAllocatorBase.Weighted)var5.next();
         tokens.add(((TokenAllocatorBase.TokenInfo)tokenInfo.value).token);
      }

      this.sortedTokens.keySet().removeAll(tokens);
   }

   public int getReplicas() {
      return 1;
   }
}
