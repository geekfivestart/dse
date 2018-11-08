package org.apache.cassandra.dht.tokenallocator;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.Map.Entry;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;

class ReplicationAwareTokenAllocator<Unit> extends TokenAllocatorBase<Unit> {
   final Multimap<Unit, Token> unitToTokens = HashMultimap.create();
   final int replicas;

   ReplicationAwareTokenAllocator(NavigableMap<Token, Unit> sortedTokens, ReplicationStrategy<Unit> strategy, IPartitioner partitioner) {
      super(sortedTokens, strategy, partitioner);
      Iterator var4 = sortedTokens.entrySet().iterator();

      while(var4.hasNext()) {
         Entry<Token, Unit> en = (Entry)var4.next();
         this.unitToTokens.put(en.getValue(), en.getKey());
      }

      this.replicas = strategy.replicas();
   }

   public int getReplicas() {
      return this.replicas;
   }

   public Collection<Token> addUnit(Unit newUnit, int numTokens) {
      assert !this.unitToTokens.containsKey(newUnit);

      if(this.unitCount() < this.replicas) {
         return this.generateSplits(newUnit, numTokens);
      } else if(numTokens > this.sortedTokens.size()) {
         return this.generateSplits(newUnit, numTokens);
      } else {
         double optTokenOwnership = this.optimalTokenOwnership(numTokens);
         Map<Object, TokenAllocatorBase.GroupInfo> groups = Maps.newHashMap();
         Map<Unit, TokenAllocatorBase.UnitInfo<Unit>> unitInfos = this.createUnitInfos(groups);
         if(groups.size() < this.replicas) {
            return this.generateSplits(newUnit, numTokens);
         } else {
            TokenAllocatorBase.UnitInfo<Unit> newUnitInfo = new TokenAllocatorBase.UnitInfo(newUnit, (double)numTokens * optTokenOwnership, groups, this.strategy);
            TokenAllocatorBase.TokenInfo<Unit> tokens = this.createTokenInfos(unitInfos, newUnitInfo.group);
            newUnitInfo.tokenCount = numTokens;
            ReplicationAwareTokenAllocator.CandidateInfo<Unit> candidates = this.createCandidates(tokens, newUnitInfo, optTokenOwnership);
            PriorityQueue<TokenAllocatorBase.Weighted<ReplicationAwareTokenAllocator.CandidateInfo<Unit>>> improvements = new PriorityQueue(this.sortedTokens.size());
            ReplicationAwareTokenAllocator.CandidateInfo candidate = candidates;

            do {
               double impr = this.evaluateImprovement(candidate, optTokenOwnership, 1.0D / (double)numTokens);
               improvements.add(new TokenAllocatorBase.Weighted(impr, candidate));
               candidate = (ReplicationAwareTokenAllocator.CandidateInfo)candidate.next;
            } while(candidate != candidates);

            ReplicationAwareTokenAllocator.CandidateInfo<Unit> bestToken = (ReplicationAwareTokenAllocator.CandidateInfo)((TokenAllocatorBase.Weighted)improvements.remove()).value;
            int vn = 1;

            while(true) {
               candidates = (ReplicationAwareTokenAllocator.CandidateInfo)bestToken.removeFrom(candidates);
               this.confirmCandidate(bestToken);
               if(vn == numTokens) {
                  return ImmutableList.copyOf(this.unitToTokens.get(newUnit));
               }

               while(true) {
                  bestToken = (ReplicationAwareTokenAllocator.CandidateInfo)((TokenAllocatorBase.Weighted)improvements.remove()).value;
                  double impr = this.evaluateImprovement(bestToken, optTokenOwnership, ((double)vn + 1.0D) / (double)numTokens);
                  TokenAllocatorBase.Weighted<ReplicationAwareTokenAllocator.CandidateInfo<Unit>> next = (TokenAllocatorBase.Weighted)improvements.peek();
                  if(next == null || impr >= next.weight) {
                     ++vn;
                     break;
                  }

                  improvements.add(new TokenAllocatorBase.Weighted(impr, bestToken));
               }
            }
         }
      }
   }

   Collection<Token> generateSplits(Unit newUnit, int numTokens) {
      Collection<Token> tokens = super.generateSplits(newUnit, numTokens);
      this.unitToTokens.putAll(newUnit, tokens);
      return tokens;
   }

   private TokenAllocatorBase.TokenInfo<Unit> createTokenInfos(Map<Unit, TokenAllocatorBase.UnitInfo<Unit>> units, TokenAllocatorBase.GroupInfo newUnitGroup) {
      TokenAllocatorBase.TokenInfo<Unit> prev = null;
      TokenAllocatorBase.TokenInfo<Unit> first = null;

      TokenAllocatorBase.TokenInfo ti;
      for(Iterator var5 = this.sortedTokens.entrySet().iterator(); var5.hasNext(); prev = ti) {
         Entry<Token, Unit> en = (Entry)var5.next();
         Token t = (Token)en.getKey();
         TokenAllocatorBase.UnitInfo<Unit> ni = (TokenAllocatorBase.UnitInfo)units.get(en.getValue());
         ti = new TokenAllocatorBase.TokenInfo(t, ni);
         first = (TokenAllocatorBase.TokenInfo)ti.insertAfter(first, prev);
      }

      TokenAllocatorBase.TokenInfo curr = first;

      do {
         this.populateTokenInfoAndAdjustUnit(curr, newUnitGroup);
         curr = (TokenAllocatorBase.TokenInfo)curr.next;
      } while(curr != first);

      return first;
   }

   private ReplicationAwareTokenAllocator.CandidateInfo<Unit> createCandidates(TokenAllocatorBase.TokenInfo<Unit> tokens, TokenAllocatorBase.UnitInfo<Unit> newUnitInfo, double initialTokenOwnership) {
      TokenAllocatorBase.TokenInfo<Unit> curr = tokens;
      ReplicationAwareTokenAllocator.CandidateInfo<Unit> first = null;
      ReplicationAwareTokenAllocator.CandidateInfo prev = null;

      ReplicationAwareTokenAllocator.CandidateInfo candidate;
      do {
         candidate = new ReplicationAwareTokenAllocator.CandidateInfo(this.partitioner.midpoint(((TokenAllocatorBase.TokenInfo)curr.prev).token, curr.token), curr, newUnitInfo);
         first = (ReplicationAwareTokenAllocator.CandidateInfo)candidate.insertAfter(first, prev);
         candidate.replicatedOwnership = initialTokenOwnership;
         this.populateCandidate(candidate);
         prev = candidate;
         curr = (TokenAllocatorBase.TokenInfo)curr.next;
      } while(curr != tokens);

      candidate.next = first;
      return first;
   }

   private void populateCandidate(ReplicationAwareTokenAllocator.CandidateInfo<Unit> candidate) {
      this.populateTokenInfo(candidate, candidate.owningUnit.group);
   }

   private void confirmCandidate(ReplicationAwareTokenAllocator.CandidateInfo<Unit> candidate) {
      TokenAllocatorBase.UnitInfo<Unit> newUnit = candidate.owningUnit;
      Token newToken = candidate.token;
      this.sortedTokens.put(newToken, newUnit.unit);
      this.unitToTokens.put(newUnit.unit, newToken);
      TokenAllocatorBase.TokenInfo<Unit> prev = candidate.prevInRing();
      TokenAllocatorBase.TokenInfo<Unit> newTokenInfo = new TokenAllocatorBase.TokenInfo(newToken, newUnit);
      newTokenInfo.replicatedOwnership = candidate.replicatedOwnership;
      newTokenInfo.insertAfter(prev, prev);
      this.populateTokenInfoAndAdjustUnit(newTokenInfo, newUnit.group);
      ReplicationAwareTokenAllocator<Unit>.ReplicationVisitor replicationVisitor = new ReplicationAwareTokenAllocator.ReplicationVisitor();

      assert newTokenInfo.next == candidate.split;

      for(TokenAllocatorBase.TokenInfo curr = (TokenAllocatorBase.TokenInfo)newTokenInfo.next; !replicationVisitor.visitedAll(); curr = (TokenAllocatorBase.TokenInfo)curr.next) {
         candidate = (ReplicationAwareTokenAllocator.CandidateInfo)candidate.next;
         this.populateCandidate(candidate);
         if(replicationVisitor.add(curr.owningUnit.group)) {
            this.populateTokenInfoAndAdjustUnit(curr, newUnit.group);
         }
      }

      replicationVisitor.clean();
   }

   private Token populateTokenInfo(TokenAllocatorBase.BaseTokenInfo<Unit, ?> token, TokenAllocatorBase.GroupInfo newUnitGroup) {
      TokenAllocatorBase.GroupInfo tokenGroup = token.owningUnit.group;
      ReplicationAwareTokenAllocator<Unit>.PopulateVisitor visitor = new ReplicationAwareTokenAllocator.PopulateVisitor();
      Token replicationThreshold = token.token;
      TokenAllocatorBase.TokenInfo curr = token.prevInRing();

      Token replicationStart;
      TokenAllocatorBase.GroupInfo currGroup;
      while(true) {
         replicationStart = curr.token;
         currGroup = curr.owningUnit.group;
         if(visitor.add(currGroup)) {
            if(visitor.visitedAll()) {
               break;
            }

            replicationThreshold = replicationStart;
            if(currGroup == tokenGroup) {
               break;
            }
         }

         curr = (TokenAllocatorBase.TokenInfo)curr.prev;
      }

      if(newUnitGroup == tokenGroup) {
         replicationThreshold = token.token;
      } else if(newUnitGroup != currGroup && visitor.seen(newUnitGroup)) {
         replicationThreshold = replicationStart;
      }

      visitor.clean();
      token.replicationThreshold = replicationThreshold;
      token.replicationStart = replicationStart;
      return replicationStart;
   }

   private void populateTokenInfoAndAdjustUnit(TokenAllocatorBase.TokenInfo<Unit> populate, TokenAllocatorBase.GroupInfo newUnitGroup) {
      Token replicationStart = this.populateTokenInfo(populate, newUnitGroup);
      double newOwnership = replicationStart.size(populate.token);
      double oldOwnership = populate.replicatedOwnership;
      populate.replicatedOwnership = newOwnership;
      populate.owningUnit.ownership += newOwnership - oldOwnership;
   }

   private double evaluateImprovement(ReplicationAwareTokenAllocator.CandidateInfo<Unit> candidate, double optTokenOwnership, double newUnitMult) {
      double tokenChange = 0.0D;
      TokenAllocatorBase.UnitInfo<Unit> candidateUnit = candidate.owningUnit;
      Token candidateEnd = candidate.token;
      ReplicationAwareTokenAllocator.UnitAdjustmentTracker<Unit> unitTracker = new ReplicationAwareTokenAllocator.UnitAdjustmentTracker(candidateUnit);
      tokenChange += this.applyOwnershipAdjustment(candidate, candidateUnit, candidate.replicationStart, candidateEnd, optTokenOwnership, unitTracker);
      ReplicationAwareTokenAllocator<Unit>.ReplicationVisitor replicationVisitor = new ReplicationAwareTokenAllocator.ReplicationVisitor();

      for(TokenAllocatorBase.TokenInfo curr = candidate.split; !replicationVisitor.visitedAll(); curr = (TokenAllocatorBase.TokenInfo)curr.next) {
         TokenAllocatorBase.UnitInfo<Unit> currUnit = curr.owningUnit;
         if(replicationVisitor.add(currUnit.group)) {
            Token replicationEnd = curr.token;
            Token replicationStart = this.findUpdatedReplicationStart(curr, candidate);
            tokenChange += this.applyOwnershipAdjustment(curr, currUnit, replicationStart, replicationEnd, optTokenOwnership, unitTracker);
         }
      }

      replicationVisitor.clean();
      double nodeChange = unitTracker.calculateUnitChange(newUnitMult, optTokenOwnership);
      return -(tokenChange + nodeChange);
   }

   private Token findUpdatedReplicationStart(TokenAllocatorBase.TokenInfo<Unit> curr, ReplicationAwareTokenAllocator.CandidateInfo<Unit> candidate) {
      return furtherStartToken(curr.replicationThreshold, candidate.token, curr.token);
   }

   private double applyOwnershipAdjustment(TokenAllocatorBase.BaseTokenInfo<Unit, ?> curr, TokenAllocatorBase.UnitInfo<Unit> currUnit, Token replicationStart, Token replicationEnd, double optTokenOwnership, ReplicationAwareTokenAllocator.UnitAdjustmentTracker<Unit> unitTracker) {
      double oldOwnership = curr.replicatedOwnership;
      double newOwnership = replicationStart.size(replicationEnd);
      double tokenCount = (double)currUnit.tokenCount;

      assert tokenCount > 0.0D;

      unitTracker.add(currUnit, newOwnership - oldOwnership);
      return (sq(newOwnership - optTokenOwnership) - sq(oldOwnership - optTokenOwnership)) / sq(tokenCount);
   }

   private double optimalTokenOwnership(int tokensToAdd) {
      return 1.0D * (double)this.replicas / (double)(this.sortedTokens.size() + tokensToAdd);
   }

   private static Token furtherStartToken(Token t1, Token t2, Token towards) {
      return t1.equals(towards)?t2:(t2.equals(towards)?t1:(t1.size(towards) > t2.size(towards)?t1:t2));
   }

   private static double sq(double d) {
      return d * d;
   }

   void removeUnit(Unit n) {
      Collection<Token> tokens = this.unitToTokens.removeAll(n);
      this.sortedTokens.keySet().removeAll(tokens);
   }

   public int unitCount() {
      return this.unitToTokens.asMap().size();
   }

   public String toString() {
      return this.getClass().getSimpleName();
   }

   static void dumpTokens(String lead, TokenAllocatorBase.BaseTokenInfo<?, ?> tokens) {
      TokenAllocatorBase.BaseTokenInfo token = tokens;

      do {
         System.out.format("%s%s: rs %s rt %s size %.2e%n", new Object[]{lead, token, token.replicationStart, token.replicationThreshold, Double.valueOf(token.replicatedOwnership)});
         token = (TokenAllocatorBase.BaseTokenInfo)token.next;
      } while(token != null && token != tokens);

   }

   private static class CandidateInfo<Unit> extends TokenAllocatorBase.BaseTokenInfo<Unit, ReplicationAwareTokenAllocator.CandidateInfo<Unit>> {
      final TokenAllocatorBase.TokenInfo<Unit> split;

      public CandidateInfo(Token token, TokenAllocatorBase.TokenInfo<Unit> split, TokenAllocatorBase.UnitInfo<Unit> owningUnit) {
         super(token, owningUnit);
         this.split = split;
      }

      TokenAllocatorBase.TokenInfo<Unit> prevInRing() {
         return (TokenAllocatorBase.TokenInfo)this.split.prev;
      }
   }

   private class PopulateVisitor extends ReplicationAwareTokenAllocator<Unit>.GroupVisitor {
      private PopulateVisitor() {
         super(null);
      }

      TokenAllocatorBase.GroupInfo prevSeen(TokenAllocatorBase.GroupInfo group) {
         return group.prevPopulate;
      }

      void setPrevSeen(TokenAllocatorBase.GroupInfo group, TokenAllocatorBase.GroupInfo prevSeen) {
         group.prevPopulate = prevSeen;
      }
   }

   private class ReplicationVisitor extends ReplicationAwareTokenAllocator<Unit>.GroupVisitor {
      private ReplicationVisitor() {
         super(null);
      }

      TokenAllocatorBase.GroupInfo prevSeen(TokenAllocatorBase.GroupInfo group) {
         return group.prevSeen;
      }

      void setPrevSeen(TokenAllocatorBase.GroupInfo group, TokenAllocatorBase.GroupInfo prevSeen) {
         group.prevSeen = prevSeen;
      }
   }

   private abstract class GroupVisitor {
      TokenAllocatorBase.GroupInfo groupChain;
      int seen;

      private GroupVisitor() {
         this.groupChain = TokenAllocatorBase.GroupInfo.TERMINATOR;
         this.seen = 0;
      }

      abstract TokenAllocatorBase.GroupInfo prevSeen(TokenAllocatorBase.GroupInfo var1);

      abstract void setPrevSeen(TokenAllocatorBase.GroupInfo var1, TokenAllocatorBase.GroupInfo var2);

      boolean add(TokenAllocatorBase.GroupInfo group) {
         if(this.prevSeen(group) != null) {
            return false;
         } else {
            ++this.seen;
            this.setPrevSeen(group, this.groupChain);
            this.groupChain = group;
            return true;
         }
      }

      boolean visitedAll() {
         return this.seen >= ReplicationAwareTokenAllocator.this.replicas;
      }

      boolean seen(TokenAllocatorBase.GroupInfo group) {
         return this.prevSeen(group) != null;
      }

      void clean() {
         TokenAllocatorBase.GroupInfo prev;
         for(TokenAllocatorBase.GroupInfo groupChain = this.groupChain; groupChain != TokenAllocatorBase.GroupInfo.TERMINATOR; groupChain = prev) {
            prev = this.prevSeen(groupChain);
            this.setPrevSeen(groupChain, (TokenAllocatorBase.GroupInfo)null);
         }

         this.groupChain = TokenAllocatorBase.GroupInfo.TERMINATOR;
      }
   }

   private static class UnitAdjustmentTracker<Unit> {
      TokenAllocatorBase.UnitInfo<Unit> unitsChain;

      UnitAdjustmentTracker(TokenAllocatorBase.UnitInfo<Unit> newUnit) {
         this.unitsChain = newUnit;
      }

      void add(TokenAllocatorBase.UnitInfo<Unit> currUnit, double diff) {
         if(currUnit.prevUsed == null) {
            assert this.unitsChain.prevUsed != null || currUnit == this.unitsChain;

            currUnit.adjustedOwnership = currUnit.ownership + diff;
            currUnit.prevUsed = this.unitsChain;
            this.unitsChain = currUnit;
         } else {
            currUnit.adjustedOwnership += diff;
         }

      }

      double calculateUnitChange(double newUnitMult, double optTokenOwnership) {
         double unitChange = 0.0D;
         TokenAllocatorBase.UnitInfo unitsChain = this.unitsChain;

         while(true) {
            double newOwnership = unitsChain.adjustedOwnership;
            double oldOwnership = unitsChain.ownership;
            double tokenCount = (double)unitsChain.tokenCount;
            double diff = ReplicationAwareTokenAllocator.sq(newOwnership / tokenCount - optTokenOwnership) - ReplicationAwareTokenAllocator.sq(oldOwnership / tokenCount - optTokenOwnership);
            TokenAllocatorBase.UnitInfo<Unit> prev = unitsChain.prevUsed;
            unitsChain.prevUsed = null;
            if(unitsChain == prev) {
               unitChange += diff * newUnitMult;
               this.unitsChain = unitsChain;
               return unitChange;
            }

            unitChange += diff;
            unitsChain = prev;
         }
      }
   }
}
