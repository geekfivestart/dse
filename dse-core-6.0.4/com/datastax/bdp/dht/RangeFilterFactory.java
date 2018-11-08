package com.datastax.bdp.dht;

import com.google.common.base.Splitter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public class RangeFilterFactory {
   private static final Splitter ROUTE_PARTITION_LIST_SPLITTER = Splitter.on(Pattern.compile("(?<!\\\\),")).trimResults();

   public RangeFilterFactory() {
   }

   public static RefiningFilter<Range<Token>> byPartition(AbstractType<?> keyValidator, IPartitioner partitioner, String filter) {
      List<Token> tokens = new LinkedList();
      Iterator var4 = ROUTE_PARTITION_LIST_SPLITTER.split(filter).iterator();

      while(var4.hasNext()) {
         String key = (String)var4.next();
         Token token = DHTUtil.decodePartitionKeyAsToken(keyValidator, partitioner, key);
         tokens.add(token);
      }

      return new RangeFilterFactory.TokenListFilter(tokens);
   }

   public static RefiningFilter<Range<Token>> byIntersectingRange(IPartitioner partitioner, String routeByRange) {
      Range<Token> range = DHTUtil.decodeInterval(partitioner, routeByRange);
      return new RangeFilterFactory.IntersectingRangeFilter(range);
   }

   public static RefiningFilter<Range<Token>> byContainedRange(IPartitioner partitioner, String routeByRange) {
      Range<Token> range = DHTUtil.decodeInterval(partitioner, routeByRange);
      return new RangeFilterFactory.RestrictingRangeFilter(range);
   }

   private static class TokenListFilter extends RefiningFilter<Range<Token>> {
      private final List<Token> allowed;

      public TokenListFilter(List<Token> allowed) {
         this.allowed = allowed;
      }

      public boolean apply(Range<Token> candidate) {
         Iterator var2 = this.allowed.iterator();

         Token token;
         do {
            if(!var2.hasNext()) {
               return false;
            }

            token = (Token)var2.next();
         } while(!candidate.contains(token));

         return true;
      }

      public String toString() {
         return String.format("%s with allowed tokens: %s", new Object[]{this.getClass().getSimpleName(), this.allowed});
      }
   }

   private static class RestrictingRangeFilter extends RefiningFilter<Range<Token>> {
      private final Range<Token> allowed;

      public RestrictingRangeFilter(Range<Token> allowed) {
         this.allowed = allowed;
      }

      public boolean apply(Range<Token> candidate) {
         return candidate.contains(this.allowed);
      }

      public Range<Token> refine(Range<Token> match) {
         return this.allowed;
      }

      public String toString() {
         return String.format("%s with allowed range: %s", new Object[]{this.getClass().getSimpleName(), this.allowed});
      }
   }

   private static class IntersectingRangeFilter extends RefiningFilter<Range<Token>> {
      private final Range<Token> allowed;

      public IntersectingRangeFilter(Range<Token> allowed) {
         this.allowed = allowed;
      }

      public boolean apply(Range<Token> candidate) {
         return candidate.intersects(this.allowed);
      }

      public String toString() {
         return String.format("%s with allowed range: %s", new Object[]{this.getClass().getSimpleName(), this.allowed});
      }
   }
}
